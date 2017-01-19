package no.rutebanken.marduk.routes.file;

import no.rutebanken.marduk.routes.file.beans.FileTypeClassifierBean;
import org.apache.commons.io.IOUtils;
import org.onebusaway.gtfs_transformer.GtfsTransformer;
import org.onebusaway.gtfs_transformer.updates.EnsureStopTimesIncreaseUpdateStrategy;
import org.onebusaway.gtfs_transformer.updates.LocalVsExpressUpdateStrategy;
import org.onebusaway.gtfs_transformer.updates.RemoveDuplicateTripsStrategy;
import org.onebusaway.gtfs_transformer.updates.RemoveRepeatedStopTimesStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class ZipFileUtils {
	private static Logger logger = LoggerFactory.getLogger(ZipFileUtils.class);

	public Set<String> listFilesInZip(File file) {
		try (ZipFile zipFile = new ZipFile(file)) {
			return zipFile.stream().filter(ze -> !ze.isDirectory()).map(ze -> ze.getName()).collect(Collectors.toSet());
		} catch (IOException e) {
			return Collections.emptySet();
		}
	}

	public Set<String> listFilesInZip(byte[] data) {
		return listFilesInZip(new ByteArrayInputStream(data));
	}

	public Set<String> listFilesInZip(InputStream inputStream) {
		Set<String> fileNames = new HashSet<>();
		try (ZipInputStream zipInputStream = new ZipInputStream(inputStream)) {
			ZipEntry zipEntry = zipInputStream.getNextEntry();
			while (zipEntry != null) {
				fileNames.add(zipEntry.getName());
				zipEntry = zipInputStream.getNextEntry();
			}
			return fileNames;
		} catch (IOException e) {
			e.printStackTrace();
			return Collections.emptySet();
		}
	}

	private static File transformGtfsFiles(File inputFile) throws Exception {

		logger.info("Transforming GTFS-file");
		long t1 = System.currentTimeMillis();

		GtfsTransformer transformer = new GtfsTransformer();
		File outputFile = File.createTempFile("marduk-cleanup", ".zip");

		transformer.setGtfsInputDirectories(Arrays.asList(inputFile));

		transformer.setOutputDirectory(outputFile);
		transformer.addTransform(new RemoveRepeatedStopTimesStrategy());
		transformer.addTransform(new RemoveDuplicateTripsStrategy());
		transformer.addTransform(new EnsureStopTimesIncreaseUpdateStrategy());
		transformer.addTransform(new LocalVsExpressUpdateStrategy());
		transformer.getReader().setOverwriteDuplicates(true);

		transformer.run();

		logger.info("Transformed GTFS-file - spent {} ms", (System.currentTimeMillis() - t1));
		return outputFile;
	}

	public static boolean zipFileContainsSingleFolder(byte[] data) {

		try {
			return zipFileContainsSingleFolder(getFile(data));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	private static boolean zipFileContainsSingleFolder(File inputFile) {
		try {
			return getZipFileIfSingleFolder(inputFile) != null;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static File rePackZipFile(byte[] data) throws IOException {

		logger.info("Repacking zipfile");
		File inputFile = getFile(data);

		ZipFile zipFile = getZipFileIfSingleFolder(inputFile);

		if (zipFile == null) {
			logger.debug("Single folder not detected");
			return inputFile;
		}

		File tmpFile = File.createTempFile("marduk-output", ".zip");
		ZipOutputStream out = new ZipOutputStream(new FileOutputStream(tmpFile));

		String directoryName = "";

		ZipInputStream zipInputStream = new ZipInputStream(new ByteArrayInputStream(data));

		ZipEntry zipEntry = zipInputStream.getNextEntry();
		while (zipEntry != null) {
			if (!zipEntry.isDirectory()) {
				InputStream inputStream = zipFile.getInputStream(zipEntry);
				ZipEntry outEntry = new ZipEntry(zipEntry.getName().replace(directoryName, ""));
				out.putNextEntry(outEntry);
				byte[] buf = new byte[inputStream.available()];
				IOUtils.readFully(inputStream, buf);
				out.write(buf);
			} else {
				directoryName = zipEntry.getName();
			}
			zipEntry = zipInputStream.getNextEntry();
		}
		out.close();

		logger.info("File written to : " + tmpFile.getAbsolutePath());

		return tmpFile;
	}

	private static File getFile(byte[] data) throws IOException {
		File inputFile = File.createTempFile("marduk-input", ".zip");

		FileOutputStream fos = new FileOutputStream(inputFile);
		fos.write(data);
		fos.close();
		return inputFile;
	}

	public static File transformGtfsFile(byte[] data) throws IOException {
		File file = getFile(data);
		if (file.exists() && file.length() > 0) {
			Set<String> filenames = (new ZipFileUtils()).listFilesInZip(IOUtils.toByteArray(new FileInputStream(file)));
			if (FileTypeClassifierBean.isGtfsZip(filenames)) {
				try {
					file = transformGtfsFiles(file);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return file;
	}

	private static ZipFile getZipFileIfSingleFolder(File inputFile) throws IOException {

		if (inputFile == null || inputFile.length() == 0) {
			return null;
		}

		ZipFile zipFile = new ZipFile(inputFile);
		boolean allFilesInSingleDirectory = false;
		Enumeration<? extends ZipEntry> entries = zipFile.entries();
		String directoryName = "";
		while (entries.hasMoreElements()) {
			ZipEntry zipEntry = entries.nextElement();
			if (zipEntry.isDirectory()) {
				allFilesInSingleDirectory = true;
				directoryName = zipEntry.getName();
			} else {
				if (!zipEntry.getName().startsWith(directoryName)) {
					allFilesInSingleDirectory = false;
					break;
				}
			}
		}

		if (allFilesInSingleDirectory) {
			return zipFile;
		}
		return null;
	}

	public static InputStream addFilesToZip(InputStream source, File[] files) {
		try {
			String name = UUID.randomUUID().toString();
			File tmpZip = File.createTempFile(name, null);
			tmpZip.delete();
			byte[] buffer = new byte[1024 * 32];
			ZipInputStream zin = new ZipInputStream(source);
			ZipOutputStream out = new ZipOutputStream(new FileOutputStream(tmpZip));

			for (int i = 0; i < files.length; i++) {
				InputStream in = new FileInputStream(files[i]);
				out.putNextEntry(new ZipEntry(files[i].getName()));
				for (int read = in.read(buffer); read > -1; read = in.read(buffer)) {
					out.write(buffer, 0, read);
				}
				out.closeEntry();
				in.close();
			}

			for (ZipEntry ze = zin.getNextEntry(); ze != null; ze = zin.getNextEntry()) {
				out.putNextEntry(ze);
				for (int read = zin.read(buffer); read > -1; read = zin.read(buffer)) {
					out.write(buffer, 0, read);
				}
				out.closeEntry();
			}

			out.close();
			return new AutoDeleteOnCloseFileInputStream(tmpZip);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

}