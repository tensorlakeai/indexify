/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { IndexAdditionResponse } from '../models/IndexAdditionResponse';
import type { IndexSearchResponse } from '../models/IndexSearchResponse';
import type { SearchRequest } from '../models/SearchRequest';
import type { SyncRepository } from '../models/SyncRepository';
import type { SyncRepositoryResponse } from '../models/SyncRepositoryResponse';
import type { TextAddRequest } from '../models/TextAddRequest';

import type { CancelablePromise } from '../core/CancelablePromise';
import { OpenAPI } from '../core/OpenAPI';
import { request as __request } from '../core/request';

export class IndexifyService {

    /**
     * @param requestBody
     * @returns IndexSearchResponse Index search results
     * @throws ApiError
     */
    public static indexSearch(
        requestBody: SearchRequest,
    ): CancelablePromise<IndexSearchResponse> {
        return __request(OpenAPI, {
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
    public static addTexts(
        requestBody: TextAddRequest,
    ): CancelablePromise<IndexAdditionResponse> {
        return __request(OpenAPI, {
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
    public static syncRepository(
        requestBody: SyncRepository,
    ): CancelablePromise<SyncRepositoryResponse> {
        return __request(OpenAPI, {
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
