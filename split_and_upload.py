import os
import uuid
import tempfile
import fitz  # PyMuPDF
import boto3
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Read environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Validate environment variables
if not all([
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AIRTABLE_API_KEY,
    AIRTABLE_BASE_ID,
    AIRTABLE_TABLE_NAME,
    S3_BUCKET_NAME
]):
    raise ValueError("❌ One or more required environment variables are missing.")

# Set up S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

def split_and_upload_pdf(pdf_path):
    print(f"📄 Opening PDF: {pdf_path}")
    doc = fitz.open(pdf_path)
    print(f"📄 Total pages: {len(doc)}")

    tickets = []

    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        single_pdf = fitz.open()
        single_pdf.insert_pdf(doc, from_page=page_num, to_page=page_num)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_page:
            single_pdf.save(tmp_page.name)
            single_pdf.close()

            filename = f"{uuid.uuid4()}.pdf"
            s3.upload_file(tmp_page.name, S3_BUCKET_NAME, filename)

            # Use direct URL (not presigned) since the bucket is public
            pdf_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{filename}"

            print(f"✅ Uploaded page {page_num + 1} to {pdf_url}")

            # Send to Airtable
            airtable_payload = {
                "fields": {
                    "Page Number": page_num + 1,
                    "Page UUID": str(uuid.uuid4()),
                    "Attachment": [
                        {
                            "url": pdf_url,
                            "filename": f"ticket-page-{page_num + 1}.pdf"
                        }
                    ]
                }
            }

            headers = {
                "Authorization": f"Bearer {AIRTABLE_API_KEY}",
                "Content-Type": "application/json",
            }

            airtable_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
            res = requests.post(airtable_url, headers=headers, json=airtable_payload)

            if res.status_code == 200:
                tickets.append({"page": page_num + 1, "pdf_url": pdf_url})
                print(f"📥 Airtable record created for page {page_num + 1}")
            else:
                print(f"❌ Airtable Error {res.status_code}: {res.text}")

            os.unlink(tmp_page.name)

    print(f"🎉 Done! Uploaded and recorded {len(tickets)} pages.")
    return {
        "success": True,
        "processed_count": len(tickets),
        "tickets": tickets,
    }
