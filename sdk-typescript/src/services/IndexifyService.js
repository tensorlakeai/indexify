"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.IndexifyService = void 0;
const OpenAPI_1 = require("../core/OpenAPI");
const request_1 = require("../core/request");
class IndexifyService {
    /**
     * @param requestBody
     * @returns IndexSearchResponse Index search results
     * @throws ApiError
     */
    static indexSearch(requestBody) {
        return (0, request_1.request)(OpenAPI_1.OpenAPI, {
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
     * @returns IndexAdditionResponse Texts were successfully added to the repository
     * @throws ApiError
     */
    static addTexts(requestBody) {
        return (0, request_1.request)(OpenAPI_1.OpenAPI, {
            method: 'POST',
            url: '/repository/add_texts',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                400: `Unable to add texts`,
            },
        });
    }
    /**
     * @param requestBody
     * @returns SyncRepositoryResponse Repository synced successfully
     * @throws ApiError
     */
    static syncRepository(requestBody) {
        return (0, request_1.request)(OpenAPI_1.OpenAPI, {
            method: 'POST',
            url: '/repository/sync',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                500: `Unable to sync repository`,
            },
        });
    }
}
exports.IndexifyService = IndexifyService;
