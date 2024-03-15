import { IExtractedMetadata } from "getindexify";

export const stringToColor = (str: string) => {
  let hash = 0;
  str.split("").forEach((char) => {
    hash = char.charCodeAt(0) + ((hash << 5) - hash);
  });
  let color = "#";
  for (let i = 0; i < 3; i++) {
    const value = (hash >> (i * 8)) & 0xff;
    color += value.toString(16).padStart(2, "0");
  }
  return color;
};

export const groupMetadataByExtractor = (
  metadataArray: IExtractedMetadata[]
): Record<string, IExtractedMetadata[]> => {
  return metadataArray.reduce((accumulator, currentItem) => {
    // Use the extractor_name as the key
    const key = currentItem.extractor_name;

    // If the key doesn't exist yet, initialize it
    if (!accumulator[key]) {
      accumulator[key] = [];
    }

    // Add the current item to the appropriate group
    accumulator[key].push(currentItem);

    return accumulator;
  }, {} as Record<string, IExtractedMetadata[]>);
};
