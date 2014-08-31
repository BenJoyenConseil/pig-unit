import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class ScriptPigUTest {

    private final String scriptPath = "./src/main/script.pig";
    private PigServer pig;

    @Before
    public void setup() throws ExecException {
        PigServer pig = null;
        if (System.getProperties().containsKey("pigunit.exectype.cluster")) {
            pig = new PigServer(ExecType.MAPREDUCE);
        } else {
            pig = new PigServer(ExecType.LOCAL);
        }
    }


    @Test
    public void queries_group_ShouldGenerateBagsOfElement_WithSameQueryString() throws IOException, ParseException {
        // Given
        String[] args = {
                "n=2",
        };

        final Cluster cluster = new Cluster(pig.getPigContext());

        PigTest test = new PigTest(scriptPath, args, pig, cluster);

        String[] input = {
                "yahoo",
                "yahoo",
                "yahoo",
                "twitter",
                "facebook",
                "facebook",
                "linkedin",
        };

        String[] expected = {
                "(yahoo,{(yahoo),(yahoo),(yahoo)})",
                "(twitter,{(twitter)})",
                "(facebook,{(facebook),(facebook)})",
                "(linkedin,{(linkedin)})",
        };

        // When
        test.runScript();

        // Then
        test.assertOutput("data", input, "queries_group", expected);
    }

    @Test
    public void testTop2Queries() throws IOException, ParseException {
        String[] args = {
                "n=2",
        };


        final Cluster cluster = new Cluster(pig.getPigContext());

        PigTest test = new PigTest(scriptPath, args, pig, cluster);

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
