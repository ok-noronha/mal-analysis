import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import java.io.IOException;

public class DeleteOutputDirectory {

    public static void deleteIfExists(Job job, Path outputPath) throws IOException {
        Configuration conf = job.getConfiguration();
        FileSystem fileSystem = outputPath.getFileSystem(conf);

        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("Output directory deleted!");
        }
    }
}
