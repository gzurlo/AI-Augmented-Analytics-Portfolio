/**
 * DataProcessor.java — High-performance batch normalisation via Java.
 *
 * <p>Demonstrates Python ↔ Java interoperability using JSON over stdio.
 *
 * <p><b>Protocol</b><br>
 * The program reads a single JSON object from stdin, processes it, and writes
 * a single JSON object to stdout.  Exit code 0 on success, 1 on error.
 *
 * <p>Input JSON schema:
 * <pre>
 * {
 *   "operation": "batch_normalize",   // required
 *   "data":      [1.0, 2.0, 3.0, ...]  // array of numbers
 * }
 * </pre>
 *
 * <p>Output JSON schema:
 * <pre>
 * {
 *   "operation":  "batch_normalize",
 *   "input_size": 5,
 *   "mean":       3.0,
 *   "std_dev":    1.4142...,
 *   "normalized": [-1.414, -0.707, 0.0, 0.707, 1.414],
 *   "min":        0.0,
 *   "max":        1.0
 * }
 * </pre>
 *
 * <p><b>Compile</b>:
 * <pre>
 *   javac java_interop/DataProcessor.java -d java_interop/
 * </pre>
 *
 * <p><b>Run standalone</b>:
 * <pre>
 *   echo '{"operation":"batch_normalize","data":[1,2,3,4,5]}' \
 *     | java -cp java_interop DataProcessor
 * </pre>
 */
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class DataProcessor {

    public static void main(String[] args) throws Exception {
        PrintStream out = new PrintStream(System.out, true, "UTF-8");
        PrintStream err = new PrintStream(System.err, true, "UTF-8");

        // Read all of stdin into a single string
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
            String result = process(input);
            out.println(result);
        } catch (Exception e) {
            err.println("{\"error\": \"" + escapeJson(e.getMessage()) + "\"}");
            System.exit(1);
        }
    }

    /**
     * Dispatch to the correct operation based on the JSON payload.
     *
     * @param json Raw JSON string from stdin.
     * @return JSON string result to write to stdout.
     */
    static String process(String json) {
        String operation = extractString(json, "operation");
        if (operation == null) {
            throw new IllegalArgumentException("Missing 'operation' field in JSON input");
        }

        switch (operation) {
            case "batch_normalize":
                return batchNormalize(json);
            default:
                throw new IllegalArgumentException("Unknown operation: " + operation);
        }
    }

    /**
     * Perform z-score (standard-score) batch normalisation on the input array.
     *
     * <p>Formula:  z_i = (x_i − μ) / σ
     *
     * <p>If σ = 0 (all values identical) every normalised value is 0.0.
     *
     * @param json JSON payload containing a "data" array.
     * @return JSON string with normalised values and descriptive statistics.
     */
    static String batchNormalize(String json) {
        double[] data = extractDoubleArray(json, "data");
        int n = data.length;
        if (n == 0) {
            throw new IllegalArgumentException("'data' array must not be empty");
        }

        // Compute mean
        double sum = 0.0;
        for (double v : data) sum += v;
        double mean = sum / n;

        // Compute variance
        double variance = 0.0;
        for (double v : data) variance += (v - mean) * (v - mean);
        variance /= n;
        double stdDev = Math.sqrt(variance);

        // Compute min / max
        double min = data[0];
        double max = data[0];
        for (double v : data) {
            if (v < min) min = v;
            if (v > max) max = v;
        }

        // Z-score normalise
        double[] normalised = new double[n];
        for (int i = 0; i < n; i++) {
            normalised[i] = stdDev > 0 ? (data[i] - mean) / stdDev : 0.0;
        }

        // Build output JSON manually (no external library needed)
        StringBuilder out = new StringBuilder();
        out.append("{");
        out.append("\"operation\": \"batch_normalize\", ");
        out.append("\"input_size\": ").append(n).append(", ");
        out.append("\"mean\": ").append(round6(mean)).append(", ");
        out.append("\"std_dev\": ").append(round6(stdDev)).append(", ");
        out.append("\"min\": ").append(round6(min)).append(", ");
        out.append("\"max\": ").append(round6(max)).append(", ");
        out.append("\"normalized\": [");
        for (int i = 0; i < normalised.length; i++) {
            out.append(round6(normalised[i]));
            if (i < normalised.length - 1) out.append(", ");
        }
        out.append("]}");
        return out.toString();
    }

    // ------------------------------------------------------------------
    // Minimal JSON helpers (no external deps)
    // ------------------------------------------------------------------

    /**
     * Extract a string value for {@code key} from a flat JSON object string.
     * Only works for simple string values (no nesting).
     */
    static String extractString(String json, String key) {
        String pattern = "\"" + key + "\"";
        int idx = json.indexOf(pattern);
        if (idx < 0) return null;
        int colon = json.indexOf(":", idx + pattern.length());
        if (colon < 0) return null;
        int valueStart = colon + 1;
        while (valueStart < json.length() && Character.isWhitespace(json.charAt(valueStart))) {
            valueStart++;
        }
        if (valueStart >= json.length()) return null;
        char first = json.charAt(valueStart);
        if (first == '"') {
            int end = json.indexOf('"', valueStart + 1);
            return end > valueStart ? json.substring(valueStart + 1, end) : null;
        }
        return null;
    }

    /**
     * Extract a JSON array of numbers for {@code key} from a flat JSON object.
     */
    static double[] extractDoubleArray(String json, String key) {
        String pattern = "\"" + key + "\"";
        int idx = json.indexOf(pattern);
        if (idx < 0) throw new IllegalArgumentException("Missing '" + key + "' field");
        int arrayStart = json.indexOf("[", idx);
        int arrayEnd   = json.indexOf("]", arrayStart);
        if (arrayStart < 0 || arrayEnd < 0) throw new IllegalArgumentException("No array found for '" + key + "'");
        String arrayContent = json.substring(arrayStart + 1, arrayEnd).trim();
        if (arrayContent.isEmpty()) return new double[0];
        String[] parts = arrayContent.split(",");
        double[] result = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Double.parseDouble(parts[i].trim());
        }
        return result;
    }

    /** Round to 6 decimal places for compact JSON output. */
    static double round6(double v) {
        return Math.round(v * 1_000_000.0) / 1_000_000.0;
    }

    /** Escape a string for embedding in JSON. */
    static String escapeJson(String s) {
        if (s == null) return "null";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
