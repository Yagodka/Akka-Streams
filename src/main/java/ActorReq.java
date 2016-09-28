import akka.actor.UntypedActor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class ActorReq extends UntypedActor {

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof GetDataMsg) {

            String text = ((GetDataMsg) message).getMsg() + System.nanoTime();

            List<CharSequence> lines = new ArrayList<>();
            lines.add(text);
            System.out.println(text);
            try {
                Files.write(Paths.get("src/main/resources/test.csv"), lines, CREATE, APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else unhandled(message);
    }
}
