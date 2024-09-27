package refit.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

public class REFITProperties {

	private Properties prop;
	private Properties defaults;

	private Set<String> accessedProperties = new HashSet<>();

	public REFITProperties() {
		this(null);
	}

	public REFITProperties(REFITProperties defaults) {
		this.defaults = (defaults != null) ? defaults.prop : new Properties();
		prop = new Properties(this.defaults);
	}

	public static REFITProperties loadFile(String fileName) {
		return loadFile(fileName, null);
	}

	public static REFITProperties loadFile(String fileName, REFITProperties defaults) {
		try (FileInputStream inputStream = new FileInputStream(fileName)) {
			InputStreamReader isr = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
			REFITProperties properties = new REFITProperties(defaults);
			properties.load(isr);
			if (defaults != null) {
				verifyOverrides(properties);
			}
			return properties;
		} catch (IOException e) {
			throw new InternalError(e);
		}
	}

	private static void verifyOverrides(REFITProperties properties) {
		Properties defaults = properties.defaults;

		final String variant_tag = ".variants";
		for (Object keyObj : properties.prop.keySet()) {
			String key = (String) keyObj;
			if (key.endsWith(variant_tag)) {
				key = key.substring(0, key.length() - variant_tag.length());
			}
			if (!defaults.containsKey(key)) {
				throw new IllegalArgumentException("Key " + key + " does not overwrite anything");
			}
		}
	}

	public void assertNoUnusedKeys() {
		HashSet<String> existingKeys = new HashSet<>();
		for (Object keyObj : prop.keySet()) {
			String key = (String) keyObj;
			existingKeys.add(key);
		}
		if (defaults != null) {
			for (Object keyObj : defaults.keySet()) {
				String key = (String) keyObj;
				existingKeys.add(key);
			}
		}

		existingKeys.removeAll(accessedProperties);
		for (String key : accessedProperties) {
			existingKeys.remove(key + ".variants");
		}

		if (!existingKeys.isEmpty()) {
			throw new AssertionError("Found unused properties: " + existingKeys);
		}
	}

	public void load(Reader reader) throws IOException {
		prop.load(reader);
	}

	private String getSafeProperty(String key) {
		String property = prop.getProperty(key);
		if (property == null) {
			throw new NoSuchElementException("Missing key: " + key);
		}
		accessedProperties.add(key);
		return property;
	}

	public String getString(String key) {
		return getSafeProperty(key);
	}

	public String getOptionalString(String key) {
		accessedProperties.add(key);
		return prop.getProperty(key);
	}

	public short getShort(String key) {
		return Short.parseShort(getSafeProperty(key));
	}

	public int getInt(String key) {
		return Integer.parseInt(getSafeProperty(key));
	}

	public float getFloat(String key) {
		return Float.parseFloat(getSafeProperty(key));
	}

	public boolean getBoolean(String key) {
		return Boolean.parseBoolean(getSafeProperty(key));
	}

	public String[] getStringArray(String key) {
		String property = getSafeProperty(key);
		if (property.trim().equals("")) {
			return new String[0];
		}
		String[] parts = property.split(",");
		for (int i = 0; i < parts.length; i++) {
			parts[i] = parts[i].trim();
		}
		return parts;
	}

	public short[] getShortArray(String key) {
		String[] parts = getStringArray(key);
		short[] array = new short[parts.length];
		for (int i = 0; i < array.length; i++) {
			array[i] = Short.parseShort(parts[i]);
		}
		return array;
	}

	public int[] getIntArray(String key) {
		String[] parts = getStringArray(key);
		int[] array = new int[parts.length];
		for (int i = 0; i < array.length; i++) {
			array[i] = Integer.parseInt(parts[i]);
		}
		return array;
	}

	public short[][] getShortArrayArray(String key) {
		// Split on ',' first, then on interior spaces ' '
		// e.g '0 1 2 3, 1 2 3 0'
		String[] parts = getStringArray(key);
		short[][] array = new short[parts.length][];
		for (int i = 0; i < array.length; i++) {
			String[] splits = parts[i].split(" +");
			array[i] = new short[splits.length];
			for (int j = 0; j < splits.length; j++) {
				array[i][j] = Short.parseShort(splits[j]);
			}
		}
		return array;
	}

	@SuppressWarnings("unchecked")
	public <T> Class<? extends T> getClass(String key, Class<T> superclass) {
		String className = getSafeProperty(key);
		Class<?> clazz;
		try {
			clazz = Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException(e);
		}

		if (!superclass.isAssignableFrom(clazz)) {
			throw new IllegalArgumentException("Class " + className + " is not assignable to " + superclass.getTypeName());
		}
		return (Class<? extends T>) clazz;
	}

}
