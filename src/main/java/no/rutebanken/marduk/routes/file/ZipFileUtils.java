/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *   https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 *
 */

package no.rutebanken.marduk.routes.file;

import no.rutebanken.marduk.exceptions.MardukException;
import no.rutebanken.marduk.exceptions.MardukZipFileEntryNameEncodingException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.zip.ZipUtil;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.MalformedInputException;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class ZipFileUtils {
    private static Logger logger = LoggerFactory.getLogger(ZipFileUtils.class);


    /**
     * Test if the given byte arrray contains a zip file.
     * The test is performed by matching the magic number at the beginning of the array with the zip file magic number
     * (PK\x03\x04). Magic numbers for empty archives (PK\x05\x06) or spanned archives (PK\x07\x08) are rejected.
     *
     * @param data
     * @return
     */
    public static boolean isZipFile(byte[] data) {
        if (data.length < 4) {
            return false;
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));) {
            return in.readInt() == 0x504b0304;
        } catch (IOException e) {
            return false;
        }
    }

    public static Set<ZipEntry> listFilesInZip(byte[] data) throws IOException {
        File tmpFile = TempFileUtils.createTempFile(data, "marduk-list-files-in-zip-", ".zip");
        Set<ZipEntry> fileList = listFilesInZip(tmpFile);
        Files.delete(tmpFile.toPath());
        return fileList;
    }

    public static Set<ZipEntry> listFilesInZip(File file) {
        try (ZipFile zipFile = new ZipFile(file)) {
            return zipFile.stream().filter(ze -> !ze.isDirectory()).collect(Collectors.toSet());
        } catch (IllegalArgumentException e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            if (rootCause instanceof MalformedInputException) {
                throw new MardukZipFileEntryNameEncodingException(e);
            } else {
                throw new MardukException(e);
            }
        } catch (IOException e) {
            throw new MardukException(e);
        }
    }

    public static File zipFilesInFolder(String folder, String targetFilePath) {
        File outputZip = new File(targetFilePath);
        ZipUtil.pack(new File(folder), outputZip);
        return outputZip;
    }

    public static byte[] extractFileFromZipFile(File zipFile, String extractFileName) {
        return ZipUtil.unpackEntry(zipFile, extractFileName);
    }

    public static void unzipFile(InputStream inputStream, String targetFolder) {
        ZipUtil.unpack(inputStream, new File(targetFolder));
    }


}