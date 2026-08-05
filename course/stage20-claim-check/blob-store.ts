import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";

export const BUCKET = process.env.BUCKET ?? "kafkaos-attachments";

// MinIO speaks the real S3 API — this is the actual AWS SDK, pointed at a
// local endpoint instead of aws.com. `forcePathStyle` is the one thing
// that differs from talking to real S3: MinIO (and most self-hosted S3-
// compatible stores) expect `http://host/bucket/key`, not the
// `bucket.host/key` virtual-hosted style AWS uses by default.
export const s3 = new S3Client({
  endpoint: process.env.S3_ENDPOINT ?? "http://localhost:9002",
  region: "us-east-1",
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env.S3_ACCESS_KEY ?? "kafkaos",
    secretAccessKey: process.env.S3_SECRET_KEY ?? "kafkaos123",
  },
});

export async function uploadBlob(key: string, body: Buffer, contentType: string): Promise<void> {
  await s3.send(new PutObjectCommand({ Bucket: BUCKET, Key: key, Body: body, ContentType: contentType }));
}

export async function downloadBlob(key: string): Promise<Buffer> {
  const result = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
  const chunks: Buffer[] = [];
  for await (const chunk of result.Body as AsyncIterable<Buffer>) {
    chunks.push(Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}
