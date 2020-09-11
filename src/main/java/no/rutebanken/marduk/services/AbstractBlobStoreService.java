package no.rutebanken.marduk.services;

import no.rutebanken.marduk.Constants;
import no.rutebanken.marduk.domain.BlobStoreFiles;
import no.rutebanken.marduk.repository.BlobStoreRepository;
import org.apache.camel.Exchange;
import org.apache.camel.Header;

import java.io.InputStream;
import java.util.Collection;

import static no.rutebanken.marduk.Constants.FILE_HANDLE;

public abstract class AbstractBlobStoreService {

    protected BlobStoreRepository repository;

    private String containerName;

    public AbstractBlobStoreService(String containerName, BlobStoreRepository repository) {
        this.containerName = containerName;
        this.repository = repository;
        this.repository.setContainerName(containerName);
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

    public void copyBlobInBucket(@Header(value = Constants.FILE_HANDLE) String sourceName, @Header(value = Constants.TARGET_FILE_HANDLE) String targetName, @Header(value = Constants.BLOBSTORE_MAKE_BLOB_PUBLIC) boolean makePublic, Exchange exchange) {
        ExchangeUtils.addHeadersAndAttachments(exchange);
        repository.copyBlob(containerName, sourceName, containerName, targetName, makePublic);
    }

    public void copyBlobToAnotherBucket(@Header(value = Constants.FILE_HANDLE) String sourceName, @Header(value = Constants.TARGET_CONTAINER) String targetContainerName, @Header(value = Constants.TARGET_FILE_HANDLE) String targetName, @Header(value = Constants.BLOBSTORE_MAKE_BLOB_PUBLIC) boolean makePublic, Exchange exchange) {
        ExchangeUtils.addHeadersAndAttachments(exchange);
        repository.copyBlob(containerName, sourceName, targetContainerName, targetName, makePublic);
    }

    public void copyAllBlobs(@Header(value = Exchange.FILE_PARENT) String sourceFolder, @Header(value = Constants.TARGET_CONTAINER) String targetContainerName, @Header(value = Constants.TARGET_FILE_PARENT) String targetFolder, @Header(value = Constants.BLOBSTORE_MAKE_BLOB_PUBLIC) boolean makePublic, Exchange exchange) {
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

}
