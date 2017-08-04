package com.di.mesa.job.jstorm.configure;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by Davi on 17/8/3.
 */
public class GenericOptionsParser {
    private static final Logger LOG = LoggerFactory.getLogger(GenericOptionsParser.class);

    static final Charset UTF8 = Charset.forName("UTF-8");

    public static final String TOPOLOGY_LIB_PATH = "topology.lib.path";
    public static final String TOPOLOGY_LIB_NAME = "topology.lib.name";

    MesaConfig conf;
    CommandLine commandLine;

    // Order in this map is important for these purposes:
    // - configuration priority
    static final LinkedHashMap<String, OptionProcessor> optionProcessors = new LinkedHashMap<>();

    public GenericOptionsParser(MesaConfig conf, String[] args) throws ParseException {
        this(conf, new Options(), args);
    }

    public GenericOptionsParser(MesaConfig conf, Options options, String[] args) throws ParseException {
        this.conf = conf;
        parseGeneralOptions(options, conf, args);
    }

    public String[] getRemainingArgs() {
        return commandLine.getArgs();
    }

    public MesaConfig getConfiguration() {
        return conf;
    }

    static Options buildGeneralOptions(Options opts) {
        Options r = new Options();

        for (Object o : opts.getOptions())
            r.addOption((Option) o);

        Option libjars =
                OptionBuilder.withArgName("paths").hasArg().withDescription("comma separated jars to be used by the submitted topology").create("libjars");
        r.addOption(libjars);
        optionProcessors.put("libjars", new LibjarsProcessor());

        Option conf = OptionBuilder.withArgName("configuration file").hasArg().withDescription("an application configuration file").create("conf");
        r.addOption(conf);
        optionProcessors.put("conf", new ConfFileProcessor());

        // Must come after `conf': this option is of higher priority
        Option extraConfig = OptionBuilder.withArgName("D").hasArg().withDescription("extra configurations (preserving types)").create("D");
        r.addOption(extraConfig);
        optionProcessors.put("D", new ExtraConfigProcessor());

        return r;
    }

    void parseGeneralOptions(Options opts, MesaConfig conf, String[] args) throws ParseException {
        opts = buildGeneralOptions(opts);
        CommandLineParser parser = new GnuParser();
        commandLine = parser.parse(opts, args, true);
        processGeneralOptions(conf, commandLine);
    }

    void processGeneralOptions(MesaConfig conf, CommandLine commandLine) throws ParseException {
        for (Map.Entry<String, OptionProcessor> e : optionProcessors.entrySet())
            if (commandLine.hasOption(e.getKey()))
                e.getValue().process(conf, commandLine);
    }

    static List<File> validateFiles(String pathList) throws IOException {
        List<File> l = new ArrayList<>();

        for (String s : pathList.split(",")) {
            File file = new File(s);
            if (!file.exists())
                throw new FileNotFoundException("File `" + file.getAbsolutePath() + "' does not exist");

            l.add(file);
        }

        return l;
    }

    public static void printGenericCommandUsage(PrintStream out) {
        String[] strs = new String[]{
                "Generic options supported are", "  -conf <conf.xml>                            load configurations from",
                "                                              <conf.xml>", "  -conf <conf.yaml>                           load configurations from",
                "                                              <conf.yaml>",
                "  -D <key>=<value>                            set <key> in configuration",
                "                                              to <value> (preserve value's type)",
                "  -libjars <comma separated list of jars>     specify comma separated",
                "                                              jars to be used by",
                "                                              the submitted topology",};
        for (String s : strs)
            out.println(s);
    }

    interface OptionProcessor {
        void process(MesaConfig conf, CommandLine commandLine) throws ParseException;
    }

    static class LibjarsProcessor implements OptionProcessor {
        @Override
        public void process(MesaConfig conf, CommandLine commandLine) throws ParseException {
            try {
                List<File> jarFiles = validateFiles(commandLine.getOptionValue("libjars"));
                Map<String, String> jars = new HashMap<>(jarFiles.size());
                List<String> names = new ArrayList<>(jarFiles.size());
                for (File f : jarFiles) {
                    jars.put(f.getName(), f.getAbsolutePath());
                    names.add(f.getName());
                }
                conf.put(TOPOLOGY_LIB_PATH, jars);
                conf.put(TOPOLOGY_LIB_NAME, names);

            } catch (IOException e) {
                throw new ParseException(e.getMessage());
            }
        }
    }

    static class ExtraConfigProcessor implements OptionProcessor {
        static final Yaml yaml = new Yaml();

        @Override
        public void process(MesaConfig conf, CommandLine commandLine) throws ParseException {
            for (String s : commandLine.getOptionValues("D")) {
                String[] keyval = s.split("=", 2);
                if (keyval.length != 2)
                    throw new ParseException("Invalid option value `" + s + "'");

                conf.putAll((Map) yaml.load(keyval[0] + ": " + keyval[1]));
            }
        }
    }

    static class ConfFileProcessor implements OptionProcessor {
        static final Yaml yaml = new Yaml();

        static Map loadYamlConf(String f) throws IOException {
            InputStreamReader reader = null;
            try {
                FileInputStream fis = new FileInputStream(f);
                reader = new InputStreamReader(fis, UTF8);
                return (Map) yaml.load(reader);
            } finally {
                if (reader != null)
                    reader.close();
            }
        }

        static Map loadConf(String f) throws IOException {
            if (f.endsWith(".yaml"))
                return loadYamlConf(f);
            throw new IOException("Unknown configuration file type: " + f + " does not end with either .yaml");
        }

        @Override
        public void process(MesaConfig conf, CommandLine commandLine) throws ParseException {
            try {
                for (String f : commandLine.getOptionValues("conf")) {
                    Map m = loadConf(f);
                    if (m == null)
                        throw new ParseException("Empty configuration file " + f);
                    conf.putAll(m);
                }
            } catch (IOException e) {
                throw new ParseException(e.getMessage());
            }
        }
    }
}
