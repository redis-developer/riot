package com.redis.riot.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.JsonLineMapper;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import com.google.cloud.spring.autoconfigure.storage.GcpStorageAutoConfiguration;
import com.google.cloud.spring.core.GcpScope;
import com.google.cloud.spring.core.UserAgentHeaderProvider;
import com.google.cloud.spring.storage.GoogleStorageResource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.StorageOptions;
import com.redis.riot.file.resource.FilenameInputStreamResource;
import com.redis.riot.file.resource.OutputStreamResource;
import com.redis.riot.file.resource.UncustomizedUrlResource;
import com.redis.riot.file.resource.XmlItemReader;
import com.redis.riot.file.resource.XmlItemReaderBuilder;
import com.redis.riot.file.resource.XmlObjectReader;
import com.redis.spring.batch.common.KeyValue;

public abstract class FileUtils {

	public static final String GS_URI_PREFIX = "gs://";

	public static final String S3_URI_PREFIX = "s3://";

	public static final Pattern EXTENSION_PATTERN = Pattern.compile("(?i)\\.(?<extension>\\w+)(?:\\.(?<gz>gz))?$");

	private FileUtils() {
	}

	/**
	 *
	 * @param filename Filename that might include a glob pattern
	 * @return List of file
	 * @throws IOException
	 */
	public static Stream<String> expand(String filename) throws IOException {
		if (isFile(filename)) {
			return expand(Paths.get(filename)).stream().map(Path::toString);
		}
		return Stream.of(filename);
	}

	public static List<Path> expand(Path path) throws IOException {
		if (Files.exists(path) || path.getParent() == null || !Files.exists(path.getParent())) {
			return Arrays.asList(path);
		}
		// Path might be glob pattern
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(path.getParent(), path.getFileName().toString())) {
			List<Path> paths = new ArrayList<>();
			stream.iterator().forEachRemaining(paths::add);
			return paths;
		}
	}

	public static Stream<String> expandAll(Iterable<String> files) {
		return StreamSupport.stream(files.spliterator(), false).flatMap(f -> {
			try {
				return FileUtils.expand(f);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	public static boolean isGzip(String file) {
		return extensionGroup(file, "gz") != null;
	}

	public static FileExtension extension(Resource resource) {
		String extension = extensionGroup(resource.getFilename(), "extension");
		if (extension == null) {
			return null;
		}
		try {
			return FileExtension.valueOf(extension.toUpperCase());
		} catch (Exception e) {
			return null;
		}
	}

	private static String extensionGroup(String file, String group) {
		Matcher matcher = EXTENSION_PATTERN.matcher(file);
		if (matcher.find()) {
			return matcher.group(group);
		}
		return null;
	}

	public static boolean isFile(String file) {
		return !(isGoogleStorageResource(file) || isAmazonS3Resource(file) || ResourceUtils.isUrl(file)
				|| isConsole(file));
	}

	public static boolean isConsole(String file) {
		return "-".equals(file);
	}

	public static boolean isAmazonS3Resource(String file) {
		return file.startsWith(S3_URI_PREFIX);
	}

	public static boolean isGoogleStorageResource(String file) {
		return file.startsWith(GS_URI_PREFIX);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T> JsonItemReader<T> jsonReader(Resource resource, Class<? super T> type) {
		JsonItemReaderBuilder<T> builder = new JsonItemReaderBuilder<>();
		builder.name(resource.getFilename() + "-json-file-reader");
		builder.resource(resource);
		JacksonJsonObjectReader<T> jsonReader = new JacksonJsonObjectReader(type);
		jsonReader.setMapper(objectMapper());
		builder.jsonObjectReader(jsonReader);
		return builder.build();
	}

	public static FlatFileItemReader<Map<String, Object>> jsonlReader(Resource resource) {
		FlatFileItemReader<Map<String, Object>> reader = new FlatFileItemReader<>();
		reader.setLineMapper(new JsonLineMapper());
		reader.setResource(resource);
		return reader;
	}

	public static ObjectMapper objectMapper() {
		ObjectMapper mapper = new ObjectMapper();
		configureMapper(mapper);
		return mapper;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> XmlItemReader<T> xmlReader(Resource resource, Class<? super T> type) {
		XmlItemReaderBuilder<T> builder = new XmlItemReaderBuilder<>();
		builder.name(resource.getFilename() + "-xml-file-reader");
		builder.resource(resource);
		XmlObjectReader<T> xmlReader = new XmlObjectReader(type);
		xmlReader.setMapper(xmlMapper());
		builder.xmlObjectReader(xmlReader);
		return builder.build();
	}

	public static XmlMapper xmlMapper() {
		XmlMapper mapper = new XmlMapper();
		configureMapper(mapper);
		return mapper;
	}

	private static void configureMapper(ObjectMapper mapper) {
		mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
		SimpleModule module = new SimpleModule();
		module.addDeserializer(KeyValue.class, new KeyValueDeserializer());
		mapper.registerModule(module);
		mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
	}

	public static FileDumpType dumpType(Resource resource) {
		FileExtension extension = extension(resource);
		if (extension == FileExtension.XML) {
			return FileDumpType.XML;
		}
		if (extension == FileExtension.JSON) {
			return FileDumpType.JSON;
		}
		throw new UnsupportedOperationException("Unsupported file extension: " + extension);
	}

	public static Resource safeInputResource(String file, FileOptions options) {
		try {
			return inputResource(file, options);
		} catch (IOException e) {
			throw new RuntimeException("Could not open file " + file, e);
		}
	}

	public static Resource inputResource(String file, FileOptions options) throws IOException {
		if (FileUtils.isConsole(file)) {
			return new FilenameInputStreamResource(System.in, "stdin", "Standard Input");
		}
		Resource resource;
		try {
			resource = resource(file, true, options);
		} catch (IOException e) {
			throw new RuntimeException("Could not read file " + file, e);
		}
		resource.getInputStream();
		if (options.isGzipped() || FileUtils.isGzip(file)) {
			GZIPInputStream gzipInputStream;
			try {
				gzipInputStream = new GZIPInputStream(resource.getInputStream());
			} catch (IOException e) {
				throw new RuntimeException("Could not open input stream", e);
			}
			return new FilenameInputStreamResource(gzipInputStream, resource.getFilename(), resource.getDescription());
		}
		return resource;
	}

	public static WritableResource outputResource(String file, FileOptions options) throws IOException {
		if (FileUtils.isConsole(file)) {
			return new OutputStreamResource(System.out, "stdout", "Standard Output");
		}
		Resource resource = resource(file, false, options);
		Assert.notNull(resource, "Could not resolve file " + file);
		Assert.isInstanceOf(WritableResource.class, resource);
		WritableResource writableResource = (WritableResource) resource;
		if (options.isGzipped() || isGzip(file)) {
			OutputStream outputStream = writableResource.getOutputStream();
			return new OutputStreamResource(new GZIPOutputStream(outputStream), resource.getFilename(),
					resource.getDescription());
		}
		return writableResource;
	}

	private static GoogleStorageResource googleStorageResource(String location, boolean readOnly,
			GoogleStorageOptions options) throws IOException {
		StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(ServiceOptions.getDefaultProjectId())
				.setHeaderProvider(new UserAgentHeaderProvider(GcpStorageAutoConfiguration.class));
		if (options.getKeyFile() != null) {
			builder.setCredentials(GoogleCredentials.fromStream(Files.newInputStream(options.getKeyFile().toPath()))
					.createScoped(gcpScope(readOnly).getUrl()));
		}
		if (options.getEncodedKey() != null) {
			builder.setCredentials(GoogleCredentials
					.fromStream(new ByteArrayInputStream(Base64.getDecoder().decode(options.getEncodedKey()))));
		}
		if (options.getProjectId() != null) {
			builder.setProjectId(options.getProjectId());
		}
		return new GoogleStorageResource(builder.build().getService(), location);
	}

	private static GcpScope gcpScope(boolean readOnly) {
		if (readOnly) {
			return GcpScope.STORAGE_READ_ONLY;
		}
		return GcpScope.STORAGE_READ_WRITE;
	}

	private static Resource resource(String location, boolean readOnly, FileOptions options) throws IOException {
		if (FileUtils.isAmazonS3Resource(location)) {
			return amazonS3Resource(location, options.getAmazonS3Options());
		}
		if (FileUtils.isGoogleStorageResource(location)) {
			return googleStorageResource(location, readOnly, options.getGoogleStorageOptions());
		}
		if (ResourceUtils.isUrl(location)) {
			return new UncustomizedUrlResource(location);
		}
		return new FileSystemResource(location);
	}

	private static Resource amazonS3Resource(String location, AmazonS3Options options) {
		AmazonS3ClientBuilder clientBuilder = AmazonS3Client.builder();
		if (options.getRegion() != null) {
			clientBuilder.withRegion(options.getRegion());
		}
		if (options.getAccessKey() != null) {
			if (options.getSecretKey() == null) {
				throw new IllegalArgumentException("Amazon S3 secret key not specified");
			}
			BasicAWSCredentials credentials = new BasicAWSCredentials(options.getAccessKey(), options.getSecretKey());
			clientBuilder.withCredentials(new AWSStaticCredentialsProvider(credentials));
		}
		AmazonS3ProtocolResolver resolver = new AmazonS3ProtocolResolver(clientBuilder);
		resolver.afterPropertiesSet();
		return resolver.resolve(location, new DefaultResourceLoader());
	}

	public static List<Resource> inputResources(List<String> files, FileOptions fileOptions) {
		return expandAll(files).map(f -> safeInputResource(f, fileOptions)).collect(Collectors.toList());
	}

}
