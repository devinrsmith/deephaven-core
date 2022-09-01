package io.deephaven;

import com.google.auto.service.AutoService;
import com.sun.management.GarbageCollectionNotificationInfo;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
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

/**
 * The Garbage-Collection Application produces two stream {@link io.deephaven.engine.table.Table tables}:
 * "notification_info" and "pools". This data is modeled after the {@link GarbageCollectionNotificationInfo} event
 * information from {@link ManagementFactory#getGarbageCollectorMXBeans()}.
 */
@AutoService(ApplicationState.Factory.class)
public final class GcApplication implements ApplicationState.Factory, NotificationListener {

    private static boolean enabled() {
        // Note: this is off-by-default until the Web UI has been updated to better handle on-by-default applications.
        // return "true".equalsIgnoreCase(System.getProperty(GcApplication.class.getName()));
        // NOTE: Do not merge this line.
        return true;
    }

    @SuppressWarnings("FieldCanBeLocal")
    private LivenessScope scope;
    private GcNotificationConsumer notificationInfoConsumer;
    private GcPoolsConsumer poolsConsumer;

    @Override
    public void handleNotification(Notification notification, Object handback) {
        if (!GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType())) {
            return;
        }
        try {
            final GarbageCollectionNotificationInfo info =
                    GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
            notificationInfoConsumer.add(info);
            poolsConsumer.add(info.getGcInfo());
        } catch (Throwable t) {
            notificationInfoConsumer.acceptFailure(t);
            poolsConsumer.acceptFailure(t);
            try {
                remove();
            } catch (ListenerNotFoundException e) {
                t.addSuppressed(e);
            }
            throw t;
        }
    }

    @Override
    public ApplicationState create(Listener listener) {
        final ApplicationState state =
                new ApplicationState(listener, GcApplication.class.getName(), "Garbage-Collection Application");
        if (!enabled()) {
            return state;
        }
        scope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            setNotificationInfo(state);
            setPools(state);
        }
        install();
        return state;
    }

    private void setNotificationInfo(ApplicationState state) {
        final String name = "notification_info";
        final SetableStreamPublisher publisher = new SetableStreamPublisher();
        // noinspection resource
        final StreamToTableAdapter adapter = new StreamToTableAdapter(GcNotificationConsumer.definition(),
                publisher, UpdateGraphProcessor.DEFAULT, name);
        notificationInfoConsumer = new GcNotificationConsumer(publisher.consumer());
        publisher.setFlushDelegate(notificationInfoConsumer::flush);

        final Table notificationInfo = adapter.table();
        state.setField(name, notificationInfo);
        state.setField(String.format("%s_stats", name), GcNotificationConsumer.stats(notificationInfo));
    }

    private void setPools(ApplicationState state) {
        final String name = "pools";
        final SetableStreamPublisher publisher = new SetableStreamPublisher();
        // noinspection resource
        final StreamToTableAdapter adapter = new StreamToTableAdapter(GcPoolsConsumer.definition(), publisher,
                UpdateGraphProcessor.DEFAULT, name);
        poolsConsumer = new GcPoolsConsumer(publisher.consumer());
        publisher.setFlushDelegate(poolsConsumer::flush);

        final Table pools = adapter.table();
        state.setField(name, pools);
        state.setField(String.format("%s_stats", name), GcPoolsConsumer.stats(pools));
    }

    private void install() {
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!(bean instanceof NotificationBroadcaster)) {
                continue;
            }
            ((NotificationBroadcaster) bean).addNotificationListener(this, null, null);
        }
    }

    private void remove() throws ListenerNotFoundException {
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!(bean instanceof NotificationBroadcaster)) {
                continue;
            }
            ((NotificationBroadcaster) bean).removeNotificationListener(this);
        }
    }
}
