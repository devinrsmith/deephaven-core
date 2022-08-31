package io.deephaven;

import com.google.auto.service.AutoService;
import com.sun.management.GarbageCollectionNotificationInfo;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.stream.SetableStreamPublisher;
import io.deephaven.stream.StreamToTableAdapter;
import io.deephaven.util.SafeCloseable;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

import static com.sun.management.GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION;

@AutoService(ApplicationState.Factory.class)
public final class GcApplication2 implements ApplicationState.Factory, NotificationListener {

    private GcNotificationTable2 gcNotificationStream;

    @Override
    public ApplicationState create(Listener listener) {
        final ApplicationState state =
                new ApplicationState(listener, GcApplication2.class.getName(), "Garbage-Collection Application");
        // todo: should we be managing this at the application state level?
        // io.deephaven.engine.liveness.LivenessManager
        final LivenessScope scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            final String name = "notification_info";
            final SetableStreamPublisher publisher = new SetableStreamPublisher();
            final StreamToTableAdapter adapter = new StreamToTableAdapter(GcNotificationTable2.definition(), publisher,
                    UpdateGraphProcessor.DEFAULT, name);
            gcNotificationStream = GcNotificationTable2.of(publisher.consumer());
            publisher.setFlushDelegate(gcNotificationStream::flush);
            state.setField(name, adapter.table());
        }
        install();
        return state;
    }


    public void install() {
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!(bean instanceof NotificationBroadcaster)) {
                continue;
            }
            ((NotificationBroadcaster) bean).addNotificationListener(this, null, null);
        }
    }

    public void remove() throws ListenerNotFoundException {
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!(bean instanceof NotificationBroadcaster)) {
                continue;
            }
            ((NotificationBroadcaster) bean).removeNotificationListener(this);
        }
    }

    @Override
    public void handleNotification(Notification notification, Object handback) {
        if (!GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType())) {
            return;
        }
        try {
            gcNotificationStream
                    .add(GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData()));
        } catch (Throwable t) {
            gcNotificationStream.acceptFailure(t);
            try {
                remove();
            } catch (ListenerNotFoundException e) {
                t.addSuppressed(e);
            }
            throw t;
        }
    }
}
