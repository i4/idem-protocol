package refit.crypto;

import java.io.*;
import java.math.BigInteger;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import net.i2p.crypto.eddsa.EdDSASecurityProvider;
import refit.config.REFITConfig;
import threshsig.Dealer;
import threshsig.GroupKey;
import threshsig.KeyShare;

public class REFITKeyManager {

	public enum KeyType {
		RSA,
		ED25519,
		THRESHOLD,
	}

	public static class ThresholdKeyPair {
		public GroupKey groupKey;
		public KeyShare[] keyShares;

		public ThresholdKeyPair(GroupKey groupKey, KeyShare[] keyShares) {
			this.groupKey = groupKey;
			this.keyShares = keyShares;
		}
	}

	static {
		// load provider if not already available
		if (Security.getProvider("EdDSA") == null) {
			Security.addProvider(new EdDSASecurityProvider());
		}
	}


	private static String getKeyPath(KeyType type, int keysize) {
		return "scripts/keys/" + type.name().toLowerCase() + "-" + keysize;
	}

	private static final Map<String, KeyPair[]> keyCache = new HashMap<>();

	public static synchronized KeyPair[] loadKey(KeyType type, int keysize) {
		String filename = getKeyPath(type, keysize);
		if (keyCache.containsKey(filename)) {
			return keyCache.get(filename);
		}
		DataInputStream dat = null;
		try {
			dat = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
		} catch (FileNotFoundException e) {
			System.err.println("Key not found, run `make keys` to fix this!");
			e.printStackTrace();
			System.exit(1);
		}

		int maxNodeID = REFITConfig.TOTAL_NR_OF_REPLICAS + REFITConfig.TOTAL_NR_OF_CLIENTS;
		KeyPair[] keyPairs = new KeyPair[maxNodeID];

		for (int i = 0; i < maxNodeID; i++) {
			try {
				byte[] privEnc = new byte[dat.readInt()];
				if (privEnc.length != dat.read(privEnc)) {
					System.err.println("Too short key file " + filename + ", run `make keys` to fix this!");
					System.exit(1);
				}
				byte[] pubEnc = new byte[dat.readInt()];
				if (pubEnc.length != dat.read(pubEnc)) {
					System.err.println("Too short key file " + filename + ", run `make keys` to fix this!");
					System.exit(1);
				}
				keyPairs[i] = decodeKey(type, privEnc, pubEnc);
			} catch (IOException | InvalidKeySpecException | NoSuchProviderException | NoSuchAlgorithmException e) {
				System.err.println("Too short/broken key file " + filename + ", run `make keys` to fix this!");
				e.printStackTrace();
				System.exit(1);
			}
		}

		keyCache.put(filename, keyPairs);
		return keyPairs;
	}

	protected static KeyPair decodeKey(KeyType type, byte[] privEnc, byte[] pubEnc) throws InvalidKeySpecException, NoSuchProviderException, NoSuchAlgorithmException {
		KeyFactory keyFactory;
		switch (type) {
		case ED25519:
			keyFactory = KeyFactory.getInstance("EdDSA", "EdDSA");
			break;
		case RSA:
			keyFactory = KeyFactory.getInstance("RSA");
			break;
		default:
			throw new IllegalArgumentException();
		}

		PKCS8EncodedKeySpec privKeySpec = new PKCS8EncodedKeySpec(privEnc);
		X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(pubEnc);

		PrivateKey privateKey = keyFactory.generatePrivate(privKeySpec);
		PublicKey publicKey = keyFactory.generatePublic(pubKeySpec);
		return new KeyPair(publicKey, privateKey);
	}

	public static ThresholdKeyPair[] loadThresholdKey() {
		String filename = getKeyPath(KeyType.THRESHOLD, 3 * REFITConfig.FAULTS_TO_TOLERATE + 1);
		DataInputStream dat = null;
		try {
			dat = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
		} catch (FileNotFoundException e) {
			System.err.println("Key not found, run `make keys` to fix this!");
			e.printStackTrace();
			System.exit(1);
		}

		int maxGroupId = 4;
		ThresholdKeyPair[] keyPairs = new ThresholdKeyPair[maxGroupId];

		for (int i = 0; i < maxGroupId; i++) {
			try {
				int k = dat.readInt();
				int l = dat.readInt();
				BigInteger mod = getBigInt(dat);
				BigInteger exp = getBigInt(dat);
				GroupKey groupKey = new GroupKey(k, l, 0, null, exp, mod);

				KeyShare[] keyShares = new KeyShare[dat.readInt()];
				for (int j = 0; j < keyShares.length; j++) {
					int id = dat.readInt();
					BigInteger secret = getBigInt(dat);
					BigInteger delta = getBigInt(dat);
					BigInteger verif = getBigInt(dat);
					BigInteger grv = getBigInt(dat);

					KeyShare keyShare = new KeyShare(id, secret, groupKey.getModulus(), delta);
					keyShare.setVerifiers(verif, grv);
					keyShares[j] = keyShare;
				}

				keyPairs[i] = new ThresholdKeyPair(groupKey, keyShares);
			} catch (IOException e) {
				System.err.println("Too short/broken key file " + filename + ", run `make keys` to fix this!");
				e.printStackTrace();
				System.exit(1);
			}
		}

		return keyPairs;
	}

	public static void generateKeys() throws NoSuchProviderException, NoSuchAlgorithmException, IOException {
		writeKeys(KeyType.RSA, 1024);
		writeKeys(KeyType.ED25519, 256);
		writeThresholdKeys();
	}

	protected static void writeKeys(KeyType type, int keysize) throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
		int maxNodeID = REFITConfig.TOTAL_NR_OF_REPLICAS + REFITConfig.TOTAL_NR_OF_CLIENTS + 30;
		System.out.println("Generating " + maxNodeID + " keys");

		String filename = getKeyPath(type, keysize);
		DataOutputStream dat = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));

		List<KeyPair> keys = IntStream.range(0, maxNodeID)
				.parallel()
				.mapToObj(value -> {
					try {
						return generateKey(type, keysize);
					} catch (NoSuchProviderException | NoSuchAlgorithmException e) {
						throw new RuntimeException(e);
					}
				}).collect(Collectors.toList());
		for (int i = 0; i < maxNodeID; i++) {
			KeyPair key = keys.get(i);
			byte[] privEnc = key.getPrivate().getEncoded();
			byte[] pubEnc = key.getPublic().getEncoded();
			dat.writeInt(privEnc.length);
			dat.write(privEnc);
			dat.writeInt(pubEnc.length);
			dat.write(pubEnc);
		}

		dat.close();
	}

	protected static KeyPair generateKey(KeyType type, int keysize) throws NoSuchProviderException, NoSuchAlgorithmException {
		KeyPairGenerator keygen;
		switch (type) {
		case ED25519:
			keygen = KeyPairGenerator.getInstance("EdDSA", "EdDSA");
			break;
		case RSA:
			keygen = KeyPairGenerator.getInstance("RSA");
			break;
		default:
			throw new IllegalArgumentException();
		}

		keygen.initialize(keysize);
		return keygen.generateKeyPair();
	}

	protected static void writeThresholdKeys() throws IOException {
		String filename = getKeyPath(KeyType.THRESHOLD, 3 * REFITConfig.FAULTS_TO_TOLERATE + 1);
		DataOutputStream dat = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));

		int maxGroupId = 4;
		for (int i = 0; i < maxGroupId; i++) {
			Dealer dealer = generateThresholdKey();
			GroupKey groupKey = dealer.getGroupKey();
			KeyShare[] shares = dealer.getShares();

			dat.writeInt(groupKey.getK());
			dat.writeInt(groupKey.getL());
			putBigInt(dat, groupKey.getModulus());
			putBigInt(dat, groupKey.getExponent());

			dat.writeInt(shares.length);
			for (KeyShare keyShare : shares) {
				dat.writeInt(keyShare.getId());
				putBigInt(dat, keyShare.getSecret());
				putBigInt(dat, keyShare.getDelta());
				putBigInt(dat, keyShare.getVerifier());
				putBigInt(dat, keyShare.getGroupVerifier());
			}
		}
		dat.close();
	}

	protected static Dealer generateThresholdKey() {
		Dealer dealer = new Dealer(1024);
		dealer.generateKeys(2 * REFITConfig.FAULTS_TO_TOLERATE + 1, 3 * REFITConfig.FAULTS_TO_TOLERATE + 1);
		return dealer;
	}

	private static void putBigInt(DataOutputStream dat, BigInteger bigint) throws IOException {
		byte[] bytes = bigint.toByteArray();
		dat.writeInt(bytes.length);
		dat.write(bytes);
	}

	private static BigInteger getBigInt(DataInputStream dat) throws IOException {
		int len = dat.readInt();
		byte[] bytes = new byte[len];
		if (bytes.length != dat.read(bytes)) {
			System.err.println("Invalid threshold key, run `make keys` to fix this");
			System.exit(1);
		}
		return new BigInteger(bytes);
	}

	public static void main(String[] args) throws NoSuchAlgorithmException, NoSuchProviderException, IOException {
		REFITKeyManager.generateKeys();
	}
}
