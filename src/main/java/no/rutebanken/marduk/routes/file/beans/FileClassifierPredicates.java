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

package no.rutebanken.marduk.routes.file.beans;

import no.rutebanken.marduk.exceptions.FileValidationException;
import no.rutebanken.marduk.exceptions.MardukException;
import no.rutebanken.marduk.exceptions.MardukZipFileEntryContentEncodingException;
import no.rutebanken.marduk.exceptions.MardukZipFileEntryNameEncodingException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.CharConversionException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.MalformedInputException;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FileClassifierPredicates {

    public static final QName NETEX_PUBLICATION_DELIVERY_QNAME = new QName("http://www.netex.org.uk/netex", "PublicationDelivery");

    private static XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();

    private static final Logger logger = LoggerFactory.getLogger(FileClassifierPredicates.class);

    public static Predicate<InputStream> firstElementQNameMatchesNetex() {
        return inputStream -> firstElementQNameMatches(NETEX_PUBLICATION_DELIVERY_QNAME).test(inputStream);
    }


    public static Predicate<InputStream> firstElementQNameMatches(QName qName) {
        return inputStream -> getFirstElementQName(inputStream)
                .orElseThrow(FileValidationException::new)
                .equals(qName);
    }

    private static Optional<QName> getFirstElementQName(InputStream inputStream) {
        XMLStreamReader streamReader = null;
        try {
            streamReader = xmlInputFactory.createXMLStreamReader(inputStream);
            while (streamReader.hasNext()) {
                int eventType = streamReader.next();
                if (eventType == XMLStreamConstants.START_ELEMENT) {
                    return Optional.of(streamReader.getName());
                } else if (eventType != XMLStreamConstants.COMMENT) {
                    // If event is neither start of element or a comment, then this is probably not a xml file.
                    break;
                }
            }
        } catch (XMLStreamException e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            if(rootCause instanceof CharConversionException) {
                throw new MardukZipFileEntryContentEncodingException(e);
            } else {
                throw new MardukException(e);
            }
        } finally {
            try {
                streamReader.close();
            } catch (XMLStreamException e) {
                throw new RuntimeException("Exception while closing", e);
            }
        }
        return Optional.empty();
    }

    public static boolean validateZipContent(InputStream inputStream, Predicate<InputStream> predicate) {
        try (ZipInputStream stream = new ZipInputStream(inputStream)) {
            ZipEntry entry;
            while ( ( entry = getNextEntry(stream)) != null && !entry.isDirectory()) {
                if (testPredicate(predicate, stream, entry)) return false;
            }
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static boolean validateZipContent(InputStream inputStream, Predicate<InputStream> predicate, String skipFileRegex) throws MalformedInputException {
        try (ZipInputStream stream = new ZipInputStream(inputStream)) {
            ZipEntry entry;
            while ((entry = getNextEntry(stream)) != null) {
                if (!entry.getName().matches(skipFileRegex)) {
                    if (testPredicate(predicate, stream, entry)) {
                        return false;
                    }
                } else {
                    logger.info("Skipped file with name {}", entry.getName());
                }
            }
            return true;

        } catch (IOException e) {
            throw new MardukException(e);
        }
    }

    /**
     * Retrieve the next entry in the ZIP file.
     * MalformedInputExceptions occurring as a result of an encoding mismatch are wrapped into
     * a {@link MardukZipFileEntryNameEncodingException}
     * @param stream the zip file as a stream
     * @return the next entry in the zip file.
     * @throws IOException
     * @throws MardukZipFileEntryNameEncodingException if the zip entry encoding does not match the default encoding (UTF-8)
     */
    private static ZipEntry getNextEntry(ZipInputStream stream) throws IOException {
        try {
            return stream.getNextEntry();
        } catch (IllegalArgumentException e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            if (rootCause instanceof MalformedInputException) {
                throw new MardukZipFileEntryNameEncodingException(e);
            } else {
                throw new MardukException(e);
            }
        }
    }

    private static boolean testPredicate(Predicate<InputStream> predicate, ZipInputStream stream, ZipEntry entry) {
    	try {
			if (!predicate.test(StreamUtils.nonClosing(stream))) {
			    String s = String.format("Entry %s with size %d is invalid.", entry.getName(), entry.getSize());
			    logger.info(s);
			    return true;
			}
		} catch(MardukZipFileEntryContentEncodingException e) {
    	    throw new MardukZipFileEntryContentEncodingException("Exception while trying to classify file "+entry.getName()+" in zip file", e);
        }
    	catch (Exception e) {
			throw new MardukException("Exception while trying to classify file "+entry.getName()+" in zip file", e);
		}
        return false;
    }
}


