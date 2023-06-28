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
import type { BaseHttpRequest } from '../core/BaseHttpRequest';

export class IndexifyService {

    constructor(public readonly httpRequest: BaseHttpRequest) {}

    /**
     * @param requestBody
     * @returns IndexSearchResponse Index search results
     * @throws ApiError
     */
    public indexSearch(
        requestBody: SearchRequest,
    ): CancelablePromise<IndexSearchResponse> {
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
     * @returns IndexAdditionResponse Texts were successfully added to the repository
     * @throws ApiError
     */
    public addTexts(
        requestBody: TextAddRequest,
    ): CancelablePromise<IndexAdditionResponse> {
        return this.httpRequest.request({
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
    public syncRepository(
        requestBody: SyncRepository,
    ): CancelablePromise<SyncRepositoryResponse> {
        return this.httpRequest.request({
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
