/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

export type TextSplitterKind = ('none' | 'new_line' | {
    /**
     * Split a document across the regex boundary
     */
    regex: {
        pattern: string;
    };
});

