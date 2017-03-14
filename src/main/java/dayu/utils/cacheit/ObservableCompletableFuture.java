package dayu.utils.cacheit;

import java.util.concurrent.CompletableFuture;

import rx.Observable;
import rx.Subscription;

public class ObservableCompletableFuture<T> extends CompletableFuture<T> {
    private final Subscription subscription;
    public ObservableCompletableFuture(Observable<T> observable) {
        subscription = observable.single().subscribe(
                this::complete,
                this::completeExceptionally
        );
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        subscription.unsubscribe();
        return super.cancel(mayInterruptIfRunning);
    }
}
