import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @author zhanghongjian
 * @Date 2019/4/8 17:43
 * @Description
 */
public class LambdaTest {

    @Test
    public void AA() {
        String[] atp = {"Rafael Nadal", "Novak Djokovic",
                "Stanislas Wawrinka",
                "David Ferrer","Roger Federer",
                "Andy Murray","Tomas Berdych",
                "Juan Martin Del Potro"};
        List<String> players =  Arrays.asList(atp);

        players.forEach((player) -> System.out.println(player + "; "));
    }

    @Test
    public void runable() {
        new Thread(() -> System.out.println("It's a lambda function!")).start();
    }
}
