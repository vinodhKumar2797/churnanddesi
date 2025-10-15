package churnanddesignate;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * CsvDesigniteChurnMapper
 *
 * Usage:
 *   java -cp ".:lib/*" methodleveldatasetcreater.CsvDesigniteChurnMapper \
 *        /path/to/outDesignate.csv /path/to/output4.csv /path/to/mapped_full_allcols.csv
 *
 * Notes:
 *  - Expects the Designite CSV to have: child_commit_id, file_path (plus metrics/smells columns).
 *  - Expects the Churn/Refactor CSV to have: child_commit, new_path, old_path (plus process & flags).
 *  - Exact join: (child_commit_id == child_commit) AND (file_path == new_path) after normalizing slashes.
 *  - Fallback join: (child_commit_id == child_commit) AND (basename(file_path) == basename(new_path)).
 *  - Output keeps:
 *      commit_id, file_path, all Designite columns (except commit/path internals),
 *      all Churn columns (except commit/path internals). Name clashes from churn get “_churn” suffix.
 */
public class CsvDesigniteChurnMapper {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java methodleveldatasetcreater.CsvDesigniteChurnMapper <outDesignate.csv> <output4.csv> <out.csv>");
            System.exit(1);
        }
        Path designiteCsv = Paths.get(args[0]);
        Path churnCsv     = Paths.get(args[1]);
        Path outCsv       = Paths.get(args[2]);

        List<Map<String,String>> designite = readCsv(designiteCsv);
        List<Map<String,String>> churn     = readCsv(churnCsv);

        // Column names we rely on (with fallbacks)
        String D_COMMIT  = pickExisting(designite, "child_commit_id", "commit_id");
        String D_FILE    = pickExisting(designite, "file_path");
        String C_COMMIT  = pickExisting(churn,     "child_commit", "commit_id");
        String C_NEW     = pickExisting(churn,     "new_path");
        String C_OLD     = pickExisting(churn,     "old_path");

        if (D_COMMIT == null || D_FILE == null || C_COMMIT == null || C_NEW == null) {
            throw new IllegalArgumentException("Required headers missing. Need in Designite: child_commit_id,file_path; in Churn: child_commit,new_path");
        }

        // Normalize path fields + add basenames
        for (Map<String,String> r : designite) {
            r.put("_file_norm", norm(r.get(D_FILE)));
            r.put("_file_base", base(r.get("_file_norm")));
        }
        for (Map<String,String> r : churn) {
            r.put("_new_norm", norm(r.get(C_NEW)));
            r.put("_old_norm", norm(r.get(C_OLD)));
            r.put("_new_base", base(r.get("_new_norm")));
        }

        // Build index for churn (by commit+new_path) and by (commit+basename)
        Map<Key2, List<Map<String,String>>> churnByCommitNew = new HashMap<>();
        Map<Key2, List<Map<String,String>>> churnByCommitBase = new HashMap<>();
        for (Map<String,String> r : churn) {
            Key2 k1 = new Key2(nz(r.get(C_COMMIT)), nz(r.get("_new_norm")));
            churnByCommitNew.computeIfAbsent(k1, k -> new ArrayList<>()).add(r);
            Key2 k2 = new Key2(nz(r.get(C_COMMIT)), nz(r.get("_new_base")));
            churnByCommitBase.computeIfAbsent(k2, k -> new ArrayList<>()).add(r);
        }

        // Join: exact then fallback (skip anything already matched by exact)
        List<Joined> exactMatches = new ArrayList<>();
        Set<Key3> seen = new HashSet<>(); // (commitId, fileNorm, newNorm) to avoid dup

        for (Map<String,String> d : designite) {
            String commit = nz(d.get(D_COMMIT));
            String fNorm  = nz(d.get("_file_norm"));
            if (commit.isEmpty() || fNorm.isEmpty()) continue;
            Key2 k = new Key2(commit, fNorm);
            for (Map<String,String> c : churnByCommitNew.getOrDefault(k, List.of())) {
                Joined j = new Joined(d, c, commit,
                        nz(c.get("_new_norm")).isEmpty() ? fNorm : c.get("_new_norm"));
                exactMatches.add(j);
                seen.add(new Key3(commit, fNorm, nz(c.get("_new_norm"))));
            }
        }

        List<Joined> fallbackMatches = new ArrayList<>();
        for (Map<String,String> d : designite) {
            String commit = nz(d.get(D_COMMIT));
            String base   = nz(d.get("_file_base"));
            String fNorm  = nz(d.get("_file_norm"));
            if (commit.isEmpty() || base.isEmpty()) continue;
            Key2 k = new Key2(commit, base);
            for (Map<String,String> c : churnByCommitBase.getOrDefault(k, List.of())) {
                Key3 maybeDup = new Key3(commit, fNorm, nz(c.get("_new_norm")));
                if (seen.contains(maybeDup)) continue; // already matched by exact
                Joined j = new Joined(d, c, commit,
                        nz(c.get("_new_norm")).isEmpty() ? fNorm : c.get("_new_norm"));
                fallbackMatches.add(j);
            }
        }

        List<Joined> all = new ArrayList<>(exactMatches);
        all.addAll(fallbackMatches);

        // Build output rows:
        // - commit_id
        // - file_path (unified)
        // - all Designite columns except internals
        // - all Churn columns except internals; if name clashes with a kept Designite column, suffix "_churn"
        Set<String> dropDes = Set.of("_file_norm","_file_base", D_COMMIT, "left_commit_id", "child_commit");
        Set<String> dropCh  = Set.of("_new_norm","_old_norm","_new_base", C_COMMIT, C_NEW, C_OLD, "parent_commit", "index");

        List<String> desCols = inferColumns(designite).stream()
                .filter(c -> !dropDes.contains(c))
                .collect(Collectors.toList());

        List<String> churnColsRaw = inferColumns(churn).stream()
                .filter(c -> !dropCh.contains(c))
                .collect(Collectors.toList());

        // clash resolution
        Set<String> clashes = new HashSet<>(desCols);
        clashes.retainAll(churnColsRaw);
        Map<String,String> churnRename = new HashMap<>();
        for (String c : churnColsRaw) churnRename.put(c, clashes.contains(c) ? c + "_churn" : c);

        // header
        List<String> header = new ArrayList<>();
        header.add("commit_id");
        header.add("file_path");
        header.addAll(desCols);
        header.addAll(churnColsRaw.stream().map(churnRename::get).toList());

        // Make rows and de-dup by (commit_id, file_path)
        LinkedHashMap<Key2, String[]> uniqueRows = new LinkedHashMap<>();
        for (Joined j : all) {
            String commit = j.commitId;
            String path   = j.unifiedPath;

            // build row map
            Map<String,String> row = new LinkedHashMap<>();
            row.put("commit_id", commit);
            row.put("file_path", path);

            for (String c : desCols) row.put(c, nz(j.des.get(c)));
            for (String c : churnColsRaw) row.put(churnRename.get(c), nz(j.churn.get(c)));

            // build array in header order
            String[] out = new String[header.size()];
            for (int i = 0; i < header.size(); i++) out[i] = nz(row.get(header.get(i)));

            uniqueRows.put(new Key2(commit, path), out); // last write wins; preserves insertion order
        }

        // write CSV
        Files.createDirectories(outCsv.getParent() == null ? Paths.get(".") : outCsv.getParent());
        try (CSVWriter w = new CSVWriter(Files.newBufferedWriter(outCsv, StandardCharsets.UTF_8))) {
            w.writeNext(header.toArray(new String[0]));
            for (String[] r : uniqueRows.values()) w.writeNext(r);
        }

        System.out.println("Mapped rows: " + uniqueRows.size());
        System.out.println("Wrote: " + outCsv.toAbsolutePath());
    }

    // ---------- helpers ----------

    private static List<Map<String,String>> readCsv(Path p) throws IOException, CsvValidationException {
        List<Map<String,String>> out = new ArrayList<>();
        try (CSVReader r = new CSVReader(Files.newBufferedReader(p, StandardCharsets.UTF_8))) {
            String[] H = r.readNext();
            if (H == null) return out;
            String[] row;
            while ((row = r.readNext()) != null) {
                Map<String,String> m = new LinkedHashMap<>();
                for (int i = 0; i < H.length; i++) {
                    String key = H[i];
                    String val = (i < row.length) ? row[i] : "";
                    m.put(key, val);
                }
                out.add(m);
            }
        }
        return out;
    }

    private static List<String> inferColumns(List<Map<String,String>> rows) {
        LinkedHashSet<String> cols = new LinkedHashSet<>();
        for (Map<String,String> m : rows) cols.addAll(m.keySet());
        return new ArrayList<>(cols);
    }

    private static String pickExisting(List<Map<String,String>> rows, String... candidates) {
        if (rows.isEmpty()) return null;
        Set<String> keys = rows.get(0).keySet();
        for (String c : candidates) if (keys.contains(c)) return c;
        // If first row didn't have it (ragged CSV), scan more
        for (Map<String,String> m : rows) {
            for (String c : candidates) if (m.containsKey(c)) return c;
        }
        return null;
    }

    private static String norm(String p) {
        if (p == null) return "";
        String s = p.replace('\\', '/');
        while (s.contains("//")) s = s.replace("//", "/");
        return s;
    }

    private static String base(String p) {
        if (p == null) return "";
        int i = p.lastIndexOf('/');
        return (i >= 0 && i < p.length()-1) ? p.substring(i+1) : p;
    }

    private static String nz(String s) { return (s == null) ? "" : s; }

    // join keys
    private static final class Key2 {
        final String a, b;
        Key2(String a, String b) { this.a = a; this.b = b; }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key2)) return false;
            Key2 k = (Key2) o;
            return Objects.equals(a, k.a) && Objects.equals(b, k.b);
        }
        @Override public int hashCode() { return Objects.hash(a, b); }
    }

    private static final class Key3 {
        final String a, b, c;
        Key3(String a, String b, String c) { this.a=a; this.b=b; this.c=c; }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key3)) return false;
            Key3 k = (Key3) o;
            return Objects.equals(a,k.a) && Objects.equals(b,k.b) && Objects.equals(c,k.c);
        }
        @Override public int hashCode() { return Objects.hash(a,b,c); }
    }

    private static final class Joined {
        final Map<String,String> des;
        final Map<String,String> churn;
        final String commitId;
        final String unifiedPath;
        Joined(Map<String,String> des, Map<String,String> churn, String commitId, String unifiedPath) {
            this.des = des; this.churn = churn; this.commitId = commitId; this.unifiedPath = unifiedPath;
        }
    }
}
