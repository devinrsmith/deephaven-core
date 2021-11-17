package io.deephaven.grpc_api.uri;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.deephaven.grpc_api.session.TicketResolver;

/**
 * Binds {@link UriTicketResolver} into the {@link TicketResolver} set.
 */
@Module
public interface UriTicketResolverModule {

    @Binds
    @IntoSet
    TicketResolver bindUriTicketResolver(UriTicketResolver resolver);
}
