"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.IndexifyService = void 0;
class IndexifyService {
    httpRequest;
    constructor(httpRequest) {
        this.httpRequest = httpRequest;
    }
    /**
     * @param requestBody
     * @returns IndexSearchResponse Index search results
     * @throws ApiError
     */
    indexSearch(requestBody) {
        return this.httpRequest.request({
            method: 'GET',
            url: '/index/search',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                500: `Unable to search index`,
            },
        });
    }
    /**
     * @param requestBody
     * @returns IndexAdditionResponse Texts were successfully added to the namespace
     * @throws ApiError
     */
    addTexts(requestBody) {
        return this.httpRequest.request({
            method: 'POST',
            url: '/namespace/add_texts',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                400: `Unable to add texts`,
            },
        });
    }
    /**
     * @param requestBody
     * @returns SyncNamespaceResponse Namespace synced successfully
     * @throws ApiError
     */
    syncNamespace(requestBody) {
        return this.httpRequest.request({
            method: 'POST',
            url: '/namespace/sync',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                500: `Unable to sync namespace`,
            },
        });
    }
}
exports.IndexifyService = IndexifyService;
