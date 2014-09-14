import org.apache.hadoop.fs.Path;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import java.io.IOException;

public class ScriptWithUDFUTest {
    private String scriptPath = "./src/main/script_with_udf.pig";

    @Test
    public void montest_avec_datafu() throws IOException, ParseException {
        // Given
        PigTest.getCluster().update(new Path("./datafu-1.2.0.jar"), new Path("datafu.jar"));
        PigTest test = new PigTest(scriptPath);
        String[] input = {"1", "2", "3", "2", "2", "2", "3", "2", "2", "1"};
         String[] expected = {"((2.0))"};

        // When
        test.runScript();

        // Then
        test.assertOutput("data", input, "median", expected);
    }
}
