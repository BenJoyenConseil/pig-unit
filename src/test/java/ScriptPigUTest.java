import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import java.io.IOException;


public class ScriptPigUTest {

    @Test
    public void top2Queries() throws IOException, ParseException {
        String[] args = {
                "n=2",
        };

        PigTest test = new PigTest("./src/main/script.pig", args);

        String[] input = {
                "yahoo",
                "yahoo",
                "yahoo",
                "twitter",
                "facebook",
                "facebook",
                "linkedin",
        };

        String[] output = {
                "(yahoo,3)",
                "(facebook,2)",
        };

        test.assertOutput("data", input, "queries_limit", output);
    }
}
