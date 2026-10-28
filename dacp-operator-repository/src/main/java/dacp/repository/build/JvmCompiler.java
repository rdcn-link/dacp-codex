package dacp.repository.build;

import java.nio.file.*;
import java.util.*;
import java.util.jar.*;
import javax.tools.ToolProvider;

/** Executed only inside the build container, with fixed paths and administrator-selected libraries. */
public final class JvmCompiler {
    public static void main(String[] args) throws Exception {
        var artifactOutput = System.out;
        System.setOut(System.err);
        Path source = Path.of("/source/src"), classes = Path.of("/output/classes");
        Files.createDirectories(classes);
        List<String> options = new ArrayList<>(List.of("-classpath", "/opt/lib/*", "-d", classes.toString()));
        boolean java = args[0].equals("java");
        if (java) options.add("-proc:none");
        try (var files = Files.walk(source)) {
            files.filter(Files::isRegularFile).filter(p -> p.toString().endsWith(java ? ".java" : ".scala"))
                 .sorted().forEach(p -> options.add(p.toString()));
        }
        if (options.size() <= (java ? 5 : 4)) throw new IllegalArgumentException("No compiler sources in src/");
        if (java) {
            var compiler = ToolProvider.getSystemJavaCompiler();
            if (compiler == null) throw new IllegalStateException("Registered build runtime needs jdk.compiler");
            if (compiler.run(null, System.err, System.err, options.toArray(String[]::new)) != 0)
                throw new IllegalStateException("Java compilation failed");
        } else {
            // Reflection avoids coupling this helper to a particular Scala compiler version.
            Class<?> module = Class.forName("scala.tools.nsc.Main$");
            Object main = module.getField("MODULE$").get(null);
            if (!((Boolean) module.getMethod("process", String[].class).invoke(main, (Object) options.toArray(String[]::new))))
                throw new IllegalStateException("Scala compilation failed");
        }
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(Path.of("/output/operator.jar")))) {
            try (var files = Files.walk(classes)) {
                for (Path file : files.filter(Files::isRegularFile).sorted().toList()) {
                    jar.putNextEntry(new JarEntry(classes.relativize(file).toString().replace('\\', '/')));
                    Files.copy(file, jar); jar.closeEntry();
                }
            }
            Path resources = Path.of("/source/resources");
            if (Files.isDirectory(resources)) try (var files = Files.walk(resources)) {
                for (Path file : files.filter(Files::isRegularFile).sorted().toList()) {
                    jar.putNextEntry(new JarEntry(resources.relativize(file).toString().replace('\\', '/')));
                    Files.copy(file, jar); jar.closeEntry();
                }
            }
        }
        Files.copy(Path.of("/output/operator.jar"), artifactOutput);
        artifactOutput.flush();
    }
}
