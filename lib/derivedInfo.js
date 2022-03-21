export const uploadedFilesUrl = deriveUploadedFilesUrl();
export const PLACEHOLDER_URL = 'https://REVIEWABLE_UPLOADED_FILES.URL';

function deriveUploadedFilesUrl() {
  if (process.env.REVIEWABLE_UPLOADED_FILES_URL) {
    return process.env.REVIEWABLE_UPLOADED_FILES_URL.replace(/\/$/, '');
  }
  if (!process.env.REVIEWABLE_UPLOADS_PROVIDER) return;
  switch (process.env.REVIEWABLE_UPLOADS_PROVIDER) {
    case 'local':
      return process.env.REVIEWABLE_HOST_URL + '/usercontent';

    case 's3': {
      let bucketUrl = 'https://s3.amazonaws.com/' + process.env.REVIEWABLE_S3_BUCKET;
      if (process.env.AWS_REGION && process.env.AWS_REGION !== 'us-east-1') {
        bucketUrl = bucketUrl.replace(/\/\/s3\./, '//s3-' + process.env.AWS_REGION + '.');
      }
      return bucketUrl;
    }

    case 'gcs':
      return 'https://storage.googleapis.com/' + process.env.REVIEWABLE_GCS_BUCKET;

    default:
      throw new Error(
        `Unknown REVIEWABLE_UPLOADS_PROVIDER: ${process.env.REVIEWABLE_UPLOADS_PROVIDER}`);
  }
}
