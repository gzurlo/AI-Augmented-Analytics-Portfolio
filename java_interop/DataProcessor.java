/**
 * DataProcessor.java — Min-max batch normalisation via Java.
 *
 * <p>Protocol: reads one JSON object from stdin, writes one JSON object to stdout.
 *
 * <p>Input:
 * <pre>{"values": [1.2, 3.4, 0.8, ...]}</pre>
 *
 * <p>Output:
 * <pre>{"min": 0.8, "max": 3.4, "mean": 1.83, "std": 1.06, "normalized": [0.15, 1.0, 0.0, ...]}</pre>
 *
 * <p><b>Compile:</b>
 * <pre>  javac java_interop/DataProcessor.java -d java_interop/</pre>
 *
 * <p><b>Run standalone:</b>
 * <pre>  echo '{"values":[1,2,3,4,5]}' | java -cp java_interop DataProcessor</pre>
 */
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;

public class DataProcessor {

    public static void main(String[] args) throws Exception {
        PrintStream out = new PrintStream(System.out, true, "UTF-8");
        PrintStream err = new PrintStream(System.err, true, "UTF-8");

        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader =
                 new BufferedReader(new InputStreamReader(System.in, "UTF-8"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        }

        String input = sb.toString().trim();
        if (input.isEmpty()) {
            err.println("{\"error\": \"empty stdin\"}");
            System.exit(1);
        }

        try {
            double[] values = parseDoubleArray(input, "values");
            String result   = process(values);
            out.println(result);
        } catch (Exception e) {
            err.println("{\"error\": \"" + escapeJson(e.getMessage()) + "\"}");
            System.exit(1);
        }
    }

    /**
     * Compute min-max normalisation and descriptive stats on {@code values}.
     *
     * <p>Normalisation formula: z_i = (x_i - min) / (max - min)
     * If all values are identical, every normalised value is 0.0.
     *
     * @param values Input numeric array (must be non-empty).
     * @return JSON string with min, max, mean, std, normalised array.
     */
    static String process(double[] values) {
        if (values.length == 0) {
            throw new IllegalArgumentException("'values' array is empty");
        }

        int n = values.length;

        // Compute min, max, mean
        double min  = values[0];
        double max  = values[0];
        double sum  = 0.0;
        for (double v : values) {
            if (v < min) min = v;
            if (v > max) max = v;
            sum += v;
        }
        double mean = sum / n;

        // Population std dev
        double variance = 0.0;
        for (double v : values) {
            variance += (v - mean) * (v - mean);
        }
        variance /= n;
        double std = Math.sqrt(variance);

        // Min-max normalisation
        double range = max - min;
        double[] normalized = new double[n];
        for (int i = 0; i < n; i++) {
            normalized[i] = range > 0 ? (values[i] - min) / range : 0.0;
        }

        // Build JSON output
        StringBuilder out = new StringBuilder("{");
        out.append("\"min\": ").append(round6(min)).append(", ");
        out.append("\"max\": ").append(round6(max)).append(", ");
        out.append("\"mean\": ").append(round6(mean)).append(", ");
        out.append("\"std\": ").append(round6(std)).append(", ");
        out.append("\"input_size\": ").append(n).append(", ");
        out.append("\"normalized\": [");
        for (int i = 0; i < normalized.length; i++) {
            out.append(round6(normalized[i]));
            if (i < normalized.length - 1) out.append(", ");
        }
        out.append("]}");
        return out.toString();
    }

    // ------------------------------------------------------------------
    // Minimal JSON helpers (no external libraries)
    // ------------------------------------------------------------------

    /** Parse a JSON array of numbers for the given key from a flat JSON object. */
    static double[] parseDoubleArray(String json, String key) {
        String marker = "\"" + key + "\"";
        int idx = json.indexOf(marker);
        if (idx < 0) throw new IllegalArgumentException("Key '" + key + "' not found");
        int arrayStart = json.indexOf("[", idx);
        int arrayEnd   = json.indexOf("]", arrayStart);
        if (arrayStart < 0 || arrayEnd < 0)
            throw new IllegalArgumentException("No array for key '" + key + "'");
        String content = json.substring(arrayStart + 1, arrayEnd).trim();
        if (content.isEmpty()) return new double[0];
        String[] parts = content.split(",");
        double[] result = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Double.parseDouble(parts[i].trim());
        }
        return result;
    }

    /** Round a double to 6 decimal places for compact JSON output. */
    static double round6(double v) {
        return Math.round(v * 1_000_000.0) / 1_000_000.0;
    }

    /** Escape a string value for safe JSON embedding. */
    static String escapeJson(String s) {
        if (s == null) return "null";
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r");
    }
}
