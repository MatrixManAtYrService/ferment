// Clock probe for the JVM: run with the source launcher, no build step.
//   java ClockProbe.java <dir> <name>
// Overwrites <dir>/<name>.txt every second with Instant.now() as the JVM sees it.
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;

public class ClockProbe {
    public static void main(String[] args) throws Exception {
        Path dir = Path.of(args[0]);
        Files.createDirectories(dir);
        Path out = dir.resolve(args[1] + ".txt");
        Path tmp = dir.resolve(args[1] + ".tmp");
        while (true) {
            Files.writeString(tmp, Instant.now().toString() + "\n");
            Files.move(tmp, out, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            Thread.sleep(1000);
        }
    }
}
