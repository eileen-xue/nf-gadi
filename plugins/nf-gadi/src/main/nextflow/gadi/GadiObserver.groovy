/*
 * Copyright 2021, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.gadi

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.trace.TraceObserver

import nextflow.processor.TaskHandler
import nextflow.trace.TraceRecord
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.nio.file.StandardCopyOption
import java.util.concurrent.ConcurrentHashMap
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.util.regex.Matcher

import static java.lang.Math.ceil


/**
 *
 * @author Wenjing Xue <wenjing.xue@anu.edu.au>, Matthew Downton <matthew.downton@anu.edu.au>
 */

@Slf4j
@CompileStatic
class GadiObserver implements TraceObserver {
    private Session session

    GadiObserver(Session session) {
        this.session = session
    }

    private String format = session.config.navigate('gadi.format', '')
    private String output = session.config.navigate('gadi.output', '')
    Path path = Paths.get(output) 
    // private boolean cached = false
    private Map<String, List<String>> cachedCSV = [:]
    private Map<String, Map<String, String>> cachedJson = [:]

    Map<String, List<Map<String, String>>> results = new ConcurrentHashMap<>()

    void addToResults(String key, List<Map<String, String>> newEntries) {
        results.compute(key) { k, existingList ->
            if (existingList == null) {
                existingList = new ArrayList<>()
            }

            synchronized (existingList) {
                existingList.addAll(newEntries)
            }
            return existingList
        }
    }
        
    // Path csv_path = Paths.get("UsageReport.csv")
    void configCheck() {
        if (format.isEmpty() && output.isEmpty()) {
            format = 'csv'
            output = 'UsageReport.csv'
            path = Paths.get(output) 
            log.info "Using default report name $output"
        } else if (!format.isEmpty() && ! output.isEmpty()) {
            if (output.endsWith(format)) {
                log.info "Save the report to $output"
            } else if (output.endsWith('json')) {
                format = 'json'
                log.info "Incorrect format setting, change it to json"
            } else if (output.endsWith('csv')) {
                format = 'csv'
                log.info "Incorrect format setting, change it to csv"
            } else {
                format = 'csv'
                output = 'UsageReport.csv'
                path = Paths.get(output) 
                log.info "Incorrect format and output settings, reset report name to $output"
            }
        } else if (!format.isEmpty()) {
            if (format == 'json') {
                output = 'UsageReport.json'
                path = Paths.get(output) 
                log.info "Using default json report name $output"
            } else {
                format = 'csv'
                output = 'UsageReport.csv'
                path = Paths.get(output) 
                log.info "Using default csv report name $output"
            }
        } else {
            if (output.endsWith('json')) {
                format = 'json'
                log.info "Report format is json"
            } else if (output.endsWith('csv')) {
                format = 'csv'
                log.info "Report format is csv"
            } else {
                format = 'csv'
                output = 'UsageReport.csv'
                path = Paths.get(output) 
                log.info "Incorrect output setting, reset report name to $output"
            }
        }
    }

    void extractCSV(String file) {
        def lines = Files.readAllLines(Paths.get(file))

        if (lines.isEmpty()) {
            println "The CSV file is empty."
            return
        }

        // def header = lines[0].split(',') 
        def dataRows = lines[1..-1]  

        dataRows.collect { line ->
            def values = line.split(',')
            cachedCSV[values[0]] = values[0..-1]
        }
    }

    void extractJson (String file) {
        def jsonContent = new String(Files.readAllBytes(path))

        Map<String, List<Map<String, String>>> jsonMap = new JsonSlurper().parseText(jsonContent) as Map

        jsonMap.each { key, valueList ->
            if (valueList instanceof List) {
                valueList.each { entry ->
                    String name = entry["name"].toString()

                    Map<String, String> values = [
                        "sus": entry["sus"].toString(), 
                        "walltime": entry["walltime"].toString(), 
                        "cputime": entry["cputime"].toString(), 
                        "cpus": entry["cpus"].toString(), 
                        "memory": entry["memory"].toString(), 
                        "efficiency": entry["efficiency"].toString(), 
                        "exitCode": entry["exitCode"].toString(), 
                        "requestedMemory": entry["requestedMemory"].toString(), 
                        "requestedWalltime": entry["requestedWalltime"].toString(),
                        "requestedJobFS": entry["requestedJobFS"].toString(),
                        "usedJobFS": entry["usedJobFS"].toString(),
                        "name": entry["name"].toString()
                    ]
                    cachedJson[name] = values
                }
            }
        }
    }


    @Override
    void onFlowBegin() {
        configCheck()

        Path original = Paths.get(output)
        if(Files.exists(original)){
            // cached = true
            log.info "Found cached report file: ${original}"

            if(format == 'csv'){
                extractCSV(output)
            } else {
                extractJson(output)
            }
            
            File tmpFile = Files.createTempFile("tempCopy_", ".tmp").toFile()

            Files.copy(original, tmpFile.toPath(), StandardCopyOption.REPLACE_EXISTING)

            log.info "Temporary file copy to: ${tmpFile.absolutePath}"

        } else {
            log.info "No cached report file found: ${original}"
        }


        if(format == 'csv') {
            List<String> headers = [
                "Name",
                "Process",
                "Service Units",
                "CPUs",
                "CPU time",
                "Used Walltime",
                "Requested Walltime",
                "Used Memory",
                "Requested Memory",
                "Used JobFS",
                "Requested JobFS",
                "Efficiency",
                "Exit Code"          
            ]

            Files.write(path, (headers.join(',') + '\n').getBytes())
        }
        
    }


    @Override
    void onFlowComplete() {
        log.info "Pipeline complete! 👋"
        // String format = config.format ?: 'csv'

        // log.info "${format}, ${output}"
        // final myJson = results.collectEntries { key, values ->
        //     def valueMap = values as Map<String, String>
        //     [
        //         (key): [
        //             "Process": valueMap['process'],
        //             "Service Units": valueMap['sus'], 
        //             "CPUs": valueMap['cpus'], 
        //             "CPU time": valueMap['cputime'],
        //             "Used Walltime": valueMap['walltime'], 
        //             "Requested Walltime": valueMap['requestedWalltime'],                    
        //             "Used Memory": valueMap['memory'], 
        //             "Requested Memory": valueMap['requestedMemory'],
        //             "Used JobFS": valueMap['usedJobFS'],
        //             "Requested JobFS": valueMap['requestedJobFS'],
        //             "Efficiency": valueMap['efficiency'],
        //             "Exit Code": valueMap['exitCode']
        //         ]
        //     ]
        // }

        // def jsonOutput = JsonOutput.toJson(results)

        // final json_path = "UsageReport.json" as Path
        // Files.write(json_path, JsonOutput.prettyPrint(JsonOutput.toJson(myJson)).bytes)
        if(format == 'json'){
            Files.write(path, JsonOutput.prettyPrint(JsonOutput.toJson(results)).bytes)
        }       
    }

    def extractValuesFromReport(String filePath) {
        def file = new File(filePath)
        def sus = null
        def walltime = null
        def cpus = null
        def memory = null
        def exitCode = null
        def cputime = null
        def efficiency = null
        def requestedMemory = null
        def requestedWalltime = null
        def requestedJobFS = null
        def usedJobFS = null
    
        file.eachLine { line ->
            Matcher susMatcher = (line =~ /Service Units:\s+(\d+\.\d+)/) 
            if (susMatcher) {
                sus = susMatcher.group(1)
            }

            Matcher walltimeMatcher = (line =~ /Walltime Used:\s+(\d{2}:\d{2}:\d{2})/)
            if (walltimeMatcher) {
                walltime = walltimeMatcher.group(1)
            }

            Matcher cputimeMatcher = (line =~ /CPU Time Used:\s+(\d{2}:\d{2}:\d{2})/)
            if (cputimeMatcher) {
                cputime = cputimeMatcher.group(1)
            }

            Matcher cpusMatcher = (line =~ /NCPUs Used:\s+(\d+)/)
            if (cpusMatcher) {
                cpus = cpusMatcher.group(1)
            }

            Matcher memoryMatcher = (line =~ /Memory Used:\s+(\d+\.\d+\wB)/)
            if (memoryMatcher.find()) {
                memory = memoryMatcher.group(1)
            }

            Matcher exitCodeMatcher = (line =~ /Exit Status:\s+(\d+)/)
            if (exitCodeMatcher.find()) {
                exitCode = exitCodeMatcher.group(1)
            }

            Matcher matcher = (line =~ /Memory Requested:\s+(\d+\.\d+\wB)/)
            if (matcher.find()) {
                requestedMemory = matcher.group(1)
            }

            matcher = (line =~ /Walltime requested:\s+(\d{2}:\d{2}:\d{2})/)
            if (matcher.find()) {
                requestedWalltime = matcher.group(1)
            }

            matcher = (line =~ /JobFS requested:\s+(\d+\.\d+\wB)/)
            if (matcher.find()) {
                requestedJobFS = matcher.group(1)
            }

            matcher = (line =~ /JobFS used:\s+(\S+)/)
            if (matcher.find()) {
                usedJobFS = matcher.group(1)
            }

        }

        // calculating the efficiency
        if (cputime != null && cputime != null) {
            def cpus_used = ceil(cpus.toDouble())
            def walltimeComps = walltime.split(':').collect { it.toInteger() }
            Double walltime_mins = (walltimeComps[0] * 60) + walltimeComps[1]+ (walltimeComps[2] / 60.0)
            def cputimeComps = cputime.split(':').collect { it.toInteger() }
            Double cputime_mins = (cputimeComps[0] * 60) + cputimeComps[1] + (cputimeComps[2] / 60.0)
            def e = (cputime_mins / walltime_mins / cpus_used) as Double
            efficiency = String.format("%.2f", e)
        }
    
        return [
                    sus: sus, 
                    walltime: walltime, 
                    cputime: cputime, 
                    cpus: cpus, 
                    memory: memory, 
                    efficiency: efficiency, 
                    exitCode: exitCode, 
                    requestedMemory: requestedMemory, 
                    requestedWalltime: requestedWalltime,
                    requestedJobFS: requestedJobFS,
                    usedJobFS: usedJobFS
                ]
    }

    @Override
    void onProcessCached(TaskHandler handler, TraceRecord trace){
        String process_name = handler.task.name

        log.info "get report from cache process"

        if(format == 'csv') {
            if(cachedCSV.containsKey(process_name)) {
                Files.write(path, (cachedCSV[process_name].join(',') + '\n').getBytes(), StandardOpenOption.APPEND)
            }          
        } else {
            addToResults(process_name.split(/\s+/)[0], [cachedJson[process_name]])
            // if(cachedJson.containsKey(process_name)) {
            //     if (results.containsKey(process_name.split(/\s+/)[0])) {
            //         results[process_name.split(/\s+/)[0]] << cachedJson[process_name]
            //     } else {
            //         results[process_name.split(/\s+/)[0]] = [cachedJson[process_name]]
            //     }
            // }
        }
    }

    
    void onProcessComplete(TaskHandler handler, TraceRecord trace){
        final jobid = (trace.get('native_id') as String).trim()
        final workdir = trace.get('workdir').toString() as String
        final report = Path.of(workdir + "/.command.log")
        // log.info "onProcessComplete was invoked! ${handler.task.name} ${report.toUriString()} ${jobid}"
        // log.info "onProcessComplete was invoked! ${handler.status} "

        Map<String, String> values = [:]

        log.info "get report from completed process"

        if (jobid.contains('gadi-pbs')) {

            int count = 0
            while (!Files.exists(report)) {
                log.info "Waiting for report file to be created: ${report}"
                sleep(10000) // Sleep for 10 seconds

                count = count + 10
                if (count >= 120) {     // If the log file isn't generated in 120s, it will break the loop
                    log.info "Cannot find log file:  ${report}"
                    break;
                }
            }

            values = extractValuesFromReport(report.toUriString()) as Map

            
            while (values.sus == null || values.walltime == null || values.cpus == null || values.memory == null) {
                sleep(10000)
                log.info "Waiting for log file to complete"
                values = extractValuesFromReport(report.toUriString()) as Map
                
                count = count + 10
                if (count >= 120) {     // If the log file isn't completed in 120s, it will break the loop
                    break;
                }
            }
        }

        String process_name = handler.task.name
        if(format == 'csv') {

            List<String> row = [
                process_name,
                process_name.split(/\s+/)[0],
                values.sus,
                values.cpus,        
                values.cputime,
                values.walltime,
                values.requestedWalltime,
                values.memory,
                values.requestedMemory,
                values.usedJobFS,
                values.requestedJobFS,
                values.efficiency,
                values.exitCode
            ]
    
            Files.write(path, (row.join(',') + '\n').getBytes(), StandardOpenOption.APPEND)
        }


        values['name'] = process_name
        // if (results.containsKey(process_name.split(/\s+/)[0])) {
        //     results[process_name.split(/\s+/)[0]] << values
        // } else {
        //     results[process_name.split(/\s+/)[0]] = [values]
        // }
        addToResults(process_name.split(/\s+/)[0], [values])
        
    }

}