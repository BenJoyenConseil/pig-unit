import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ScriptPigOrderAlphabetTest {

    private String scriptPath = "./src/main/script_order_alphabet.pig";
    private PigServer pig;
    private Cluster cluster;

    @Before
    public void setup() throws ExecException {
        pig = null;
        if (System.getProperties().containsKey("pigunit.exectype.cluster")) {
            System.out.println("Using cluster mode");
            pig = new PigServer(ExecType.MAPREDUCE);
        } else {
            System.out.println("Using default local mode");
            pig = new PigServer(ExecType.LOCAL);
        }
        cluster = new Cluster(pig.getPigContext());
    }

    @Test
    public void data_ordered_shouldOrderElementsBy_Alphabet() throws IOException, ParseException {
        // Given
        String[] args = {};
        PigTest test = new PigTest(scriptPath, args, pig, cluster);

        String[] input = {
                "yahoo",
                "twitter",
                "facebook",
                "linkedin",
        };

        String[] expected = {
                "(facebook)",
                "(linkedin)",
                "(twitter)",
                "(yahoo)",
        };

        // When
        test.runScript();

        // Then
        test.assertOutput("data", input, "data_ordered", expected);
    }
}

