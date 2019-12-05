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

package no.rutebanken.marduk.services;

import com.google.cloud.storage.Storage;
import no.rutebanken.marduk.Constants;
import no.rutebanken.marduk.domain.BlobStoreFiles;
import no.rutebanken.marduk.repository.BlobStoreRepository;
import org.apache.camel.Exchange;
import org.apache.camel.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.InputStream;
import java.util.Collection;

import static no.rutebanken.marduk.Constants.FILE_HANDLE;

@Service
public class BlobStoreService {

	@Autowired
	BlobStoreRepository repository;

	@Autowired
	Storage storage;

	@Value("${blobstore.gcs.container.name}")
	String containerName;

	@PostConstruct
	public void init() {
		repository.setStorage(storage);
		repository.setContainerName(containerName);
	}

	public BlobStoreFiles listBlobsInFolder(@Header(value = Exchange.FILE_PARENT) String folder, Exchange exchange) {
		ExchangeUtils.addHeadersAndAttachments(exchange);
		return repository.listBlobs(folder + "/");
	}

	public BlobStoreFiles listBlobsInFolders(@Header(value = Constants.FILE_PARENT_COLLECTION) Collection<String> folders, Exchange exchange) {
		ExchangeUtils.addHeadersAndAttachments(exchange);
		return repository.listBlobs(folders);
	}


	public BlobStoreFiles listBlobs(@Header(value = Constants.CHOUETTE_REFERENTIAL) String referential, Exchange exchange) {
		ExchangeUtils.addHeadersAndAttachments(exchange);
		return repository.listBlobs(Constants.BLOBSTORE_PATH_INBOUND + referential + "/");
	}

	public BlobStoreFiles listBlobsFlat(@Header(value = Constants.CHOUETTE_REFERENTIAL) String referential, Exchange exchange) {
		ExchangeUtils.addHeadersAndAttachments(exchange);
		return repository.listBlobsFlat(Constants.BLOBSTORE_PATH_INBOUND + referential + "/");
	}

	public InputStream getBlob(@Header(value = Constants.FILE_HANDLE) String name, Exchange exchange) {
		ExchangeUtils.addHeadersAndAttachments(exchange);
		return repository.getBlob(name);
	}

	public void uploadBlob(@Header(value = Constants.FILE_HANDLE) String name,
			                      @Header(value = Constants.BLOBSTORE_MAKE_BLOB_PUBLIC) boolean makePublic, InputStream inputStream, Exchange exchange) {
		ExchangeUtils.addHeadersAndAttachments(exchange);
		repository.uploadBlob(name, inputStream, makePublic);
	}

	public void copyBlob(@Header(value = Constants.FILE_HANDLE) String sourceName, @Header(value = Constants.TARGET_FILE_HANDLE) String targetName,  @Header(value = Constants.BLOBSTORE_MAKE_BLOB_PUBLIC) boolean makePublic, Exchange exchange) {
		ExchangeUtils.addHeadersAndAttachments(exchange);
		repository.copyBlob(sourceName, targetName, makePublic);
	}

	public void copyAllBlobs(@Header(value = Exchange.FILE_PARENT) String sourceFolder, @Header(value = Constants.TARGET_CONTAINER) String targetContainerName,  @Header(value = Constants.TARGET_FILE_PARENT) String targetFolder,  @Header(value = Constants.BLOBSTORE_MAKE_BLOB_PUBLIC) boolean makePublic, Exchange exchange) {
		ExchangeUtils.addHeadersAndAttachments(exchange);
		repository.copyAllBlobs(containerName, sourceFolder, targetContainerName, targetFolder, makePublic);
	}

	public boolean deleteBlob(@Header(value = FILE_HANDLE) String name, Exchange exchange) {
		ExchangeUtils.addHeadersAndAttachments(exchange);
		return repository.delete(name);
	}

	public boolean deleteAllBlobsInFolder(@Header(value = Exchange.FILE_PARENT) String folder, Exchange exchange) {
		ExchangeUtils.addHeadersAndAttachments(exchange);
		return repository.deleteAllFilesInFolder(folder);
	}

	public void uploadBlob(String name, boolean makePublic, InputStream inputStream) {
		repository.uploadBlob(name, inputStream, makePublic);
	}

}
