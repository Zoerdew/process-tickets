import requests
import io
import pdfplumber
import openai
import os
import json
import tempfile
from urllib.parse import quote
from flask import Flask, request, jsonify, render_template_string

app = Flask(__name__)
print("Starting Flask app with upload routes...")

# Load environment variables
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID")
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
AIRTABLE_TABLE_NAME = os.environ.get("AIRTABLE_TABLE_NAME")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY

def clean_openai_response(text):
    if text.startswith("```json"):
        text = text[len("```json"):].strip()
    if text.startswith("```"):
        text = text[len("```"):].strip()
    if text.endswith("```"):
        text = text[:-len("```")].strip()
    return text

def sanitize_fields(fields):
    cleaned = {}
    for k, v in fields.items():
        if isinstance(v, str) and v.strip() == "":
            cleaned[k] = None
        else:
            cleaned[k] = v
    return cleaned

@app.route("/process-ticket", methods=["POST"])
def process_ticket():
    data = request.json
    record_id = data.get("recordId")
    if not record_id:
        return jsonify({"error": "Missing recordId"}), 400

    # Ensure AIRTABLE_TABLE_NAME is a string and encode it for URL
    table_name = AIRTABLE_TABLE_NAME
    if isinstance(table_name, bytes):
        table_name = table_name.decode('utf-8')
    elif not isinstance(table_name, str):
        table_name = str(table_name)
    table_name_encoded = quote(table_name)

    airtable_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_name_encoded}/{record_id}"

    print(f"Fetching Airtable URL: {airtable_url}")

    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    response = requests.get(airtable_url, headers=headers)
    if response.status_code != 200:
        return jsonify({"error": f"Failed to fetch Airtable record: {response.text}"}), 500

    record = response.json()
    fields = record.get("fields", {})

    # Get PDF URL from attachment field
    attachments = fields.get("Attachment")
    if not attachments:
        return jsonify({"error": "No PDF attachment found"}), 400
    pdf_url = attachments[0].get("url")

    # Download PDF bytes
    pdf_response = requests.get(pdf_url)
    if pdf_response.status_code != 200:
        return jsonify({"error": "Failed to download PDF"}), 500
    pdf_bytes = pdf_response.content

    # Extract text from PDF
    text = ""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"

    # Get ticket text from record
    ticket_text = fields.get("Ticket Text", "")

    # DEBUG: print texts sent to OpenAI
    print("Ticket Text:")
    print(ticket_text)
    print("Extracted PDF Text:")
    print(text)

    # Build prompt for OpenAI with example output
    prompt = f"""
You are a helpful assistant that extracts structured ticket information from text.

Extract the following fields and return a JSON object with these exact keys:
Show Name, Show Date, Section, Row, Seat, Agent Order ID, Page Number, Venue, Price, Time.

If a field is missing in the text, return its value as an empty string.

Example output:

{{
  "Show Name": "Hamilton",
  "Show Date": "2025-07-01",
  "Section": "Orchestra",
  "Row": "B",
  "Seat": "12",
  "Agent Order ID": "123456789",
  "Page Number": "1",
  "Venue": "Richard Rodgers Theatre",
  "Price": "$120",
  "Time": "7:30 PM"
}}

Ticket Text:
{ticket_text}

PDF Text:
{text}
"""

    # Call OpenAI GPT
    try:
        completion = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=700,
        )
        gpt_response_text = completion.choices[0].message.content.strip()
    except Exception as e:
        return jsonify({"error": f"OpenAI API error: {str(e)}"}), 500

    # Clean and parse JSON safely
    cleaned_response = clean_openai_response(gpt_response_text)
    try:
        structured_data = json.loads(cleaned_response)
    except json.JSONDecodeError:
        structured_data = {}
        print("Failed to parse JSON from OpenAI response:")
        print(cleaned_response)

    # Sanitize fields to replace empty strings with None for Airtable
    cleaned_fields = sanitize_fields(structured_data)
    update_payload = {"fields": cleaned_fields}

    print("Updating Airtable with these fields:")
    print(json.dumps(update_payload, indent=2))

    update_response = requests.patch(
        airtable_url,
        headers={**headers, "Content-Type": "application/json"},
        json=update_payload,
    )

    print("Airtable update response status:", update_response.status_code)
    print("Airtable update response text:", update_response.text)

    if update_response.status_code != 200:
        return jsonify({"error": f"Failed to update Airtable record: {update_response.text}"}), 500

    return jsonify({"message": "Ticket processed successfully", "extracted_fields": cleaned_fields})

# -------------------------
# Upload routes for multipage PDF
# -------------------------

@app.route('/upload', methods=['GET'])
def upload_form():
    return render_template_string('''
        <h2>Upload Full Ticket PDF</h2>
        <form method="POST" action="/upload" enctype="multipart/form-data">
          <input type="file" name="pdf_file" accept="application/pdf" required>
          <button type="submit">Upload PDF</button>
        </form>
    ''')

@app.route('/upload', methods=['POST'])
def upload_pdf():
    if 'pdf_file' not in request.files:
        return "No file part", 400
    file = request.files['pdf_file']
    if file.filename == '':
        return "No selected file", 400

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(file.read())
        tmp_path = tmp.name

    print(f"Received PDF saved at {tmp_path}")

    # Call your PDF splitting and upload function here
    from split_and_upload import split_and_upload_pdf
    result = split_and_upload_pdf(tmp_path)

    if result and result.get("success"):
        return f"✅ Uploaded {result['processed_count']} ticket pages."
    else:
        return "❌ Failed to process PDF", 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=True)
