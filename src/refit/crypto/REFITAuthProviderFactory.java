package refit.crypto;

// Interface is for example implemented by REFITAsyncSignature::new
// Store the resulting factory implementing lambda in a static variable and reuse it
// public static final REFITAuthProviderFactory<REFITAsyncSignature> factory = REFITAsyncSignature::new;
//
@FunctionalInterface
public interface REFITAuthProviderFactory<T> {
	T createProvider(short id, REFITMessageAuthentication mac);
}
