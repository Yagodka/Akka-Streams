import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import akka.actor.Props;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

public class Scheduler {
    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create();
        System.out.println("START");
        system.scheduler().scheduleOnce(
                Duration.create(5, TimeUnit.SECONDS),
                () -> System.out.println("SCHEDULE ONCE " + System.nanoTime()),
                system.dispatcher());

        FiniteDuration initialDelay = Duration.Zero();
        FiniteDuration frequency = Duration.create(1, TimeUnit.SECONDS);
        ActorRef actorRef = system.actorOf(Props.create(ActorReq.class));
        system.scheduler().schedule(initialDelay, frequency, actorRef, new GetDataMsg("SCHEDULE "), system.dispatcher(), ActorRef.noSender());
    }
}