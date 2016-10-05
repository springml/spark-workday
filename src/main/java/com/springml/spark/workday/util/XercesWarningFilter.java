package com.springml.spark.workday.util;

import java.io.PrintStream;

/**
 *Temporary hack to hide Xerces warning
 */
public class XercesWarningFilter extends PrintStream {
    private static final String BEGIN_SIG
            = "Warning:  org.apache.xerces.parsers.SAXParser:";
    private static final String END_SIG = "is not recognized.";

    private static PrintStream STDOUT;
    private static PrintStream STDERR;

    private XercesWarningFilter(PrintStream printStream) {
        super(printStream);
    }

    private static boolean initialized = false;
    public static synchronized void start() {
        if (!initialized) {
            STDOUT = System.out;
            STDERR = System.err;
            initialized = true;
        }
        System.setOut(new XercesWarningFilter(STDOUT));
        System.setErr(new XercesWarningFilter(STDERR));
    }

    public static synchronized void stop() {
        if (initialized) {
            System.setOut(STDOUT);
            System.setErr(STDERR);
        }
    }

    @Override
    public void println(String s) {
        if (!(s.startsWith(BEGIN_SIG) && s.endsWith(END_SIG))) {
            super.print(s);
        }
    }
}
