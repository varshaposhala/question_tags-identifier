import streamlit as st
import pandas as pd
import json
import requests
import re
import os
import tempfile
import shutil
import zipfile
import io # Added for reading uploaded files in memory
st.set_page_config(layout="wide", page_title="Question Tag Validator")

# --- Constants ---
S3_TOPIC_URL = "https://nxtwave-assessments-backend-nxtwave-media-static.s3.ap-south-1.amazonaws.com/topin_config_prod/static/static_content.json"
REQUIRED_TAGS = {
    "COMMON": ["NIAT", "IN_OFFLINE_EXAM", "POOL_1"],
    "DIFFICULTY": ["DIFFICULTY_EASY", "DIFFICULTY_MEDIUM", "DIFFICULTY_HARD"],
    "SOURCE": "SOURCE_",
}

# Regex for a UUID (standard format) - compiled once
UUID_REGEX = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$')

def format_tag_name(tag_input, prefix):
    """
    Formats a tag name by:
    1. Adding the appropriate prefix (COURSE_, MODULE_, UNIT_)
    2. Replacing spaces and special characters with underscores
    3. Collapsing multiple underscores to single underscore
    """
    if not tag_input or not tag_input.strip():
        return ""

    tag_input = tag_input.strip()
    
    # Remove existing prefix if present
    if tag_input.startswith(prefix):
        tag_input = tag_input[len(prefix):]

    # Replace spaces and special characters with underscores
    formatted_tag = re.sub(r'[^a-zA-Z0-9_]', '_', tag_input)

    # Collapse multiple underscores to single
    formatted_tag = re.sub(r'_+', '_', formatted_tag)

    # Remove leading/trailing underscores
    formatted_tag = formatted_tag.strip('_')

    # Add prefix
    if formatted_tag:
        return f"{prefix}{formatted_tag}"
    else:
        return ""

# --- Initialize session state for formatted tags ---
if 'formatted_course_tag' not in st.session_state:
    st.session_state.formatted_course_tag = ""
if 'formatted_module_tag' not in st.session_state:
    st.session_state.formatted_module_tag = ""
if 'formatted_unit_tag' not in st.session_state:
    st.session_state.formatted_unit_tag = ""

# --- Caching unit-subtopic map ---
@st.cache_resource
def fetch_unit_subtopic_map():
    """Fetches the unit-subtopic mapping from S3."""
    try:
        response = requests.get(S3_TOPIC_URL)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        data = response.json()
        unit_to_subtopics = {}
        for course in data.get("data", []):
            for topic in course.get("topics", []):
                for sub in topic.get("sub_topics", []):
                    unit_tag = sub.get("unit_tag")
                    sub_tag = sub.get("tag")
                    if unit_tag:
                        unit_to_subtopics.setdefault(unit_tag, []).append(sub_tag)
        return unit_to_subtopics
    except requests.exceptions.RequestException as e:
        st.error(f"Network error fetching unit-subtopic mapping: {e}. Please check your internet connection or the S3 URL.")
        return {}
    except json.JSONDecodeError as e:
        st.error(f"Error decoding JSON from S3 URL: {e}. The S3 content might be malformed.")
        return {}
    except Exception as e:
        st.error(f"An unexpected error occurred while fetching unit-subtopic mapping: {e}")
        return {}

@st.cache_resource
def fetch_topic_subtopic_tags_code_analysis():
    """
    Fetch valid TOPIC_ and SUB_TOPIC_ tags from 'CODE_ANALYSIS' section in static JSON.
    """
    try:
        response = requests.get(S3_TOPIC_URL)
        response.raise_for_status()
        data = response.json()

        topic_tags = set()
        sub_topic_tags = set()

        code_analysis_data = data.get("CODE_ANALYSIS", [])
        for topic in code_analysis_data:
            topic_val = topic.get("topic_name", {}).get("value")
            if topic_val:
                topic_tags.add(topic_val)

            for sub in topic.get("sub_topics", []):
                sub_val = sub.get("sub_topic_name", {}).get("value")
                if sub_val:
                    sub_topic_tags.add(sub_val)

        return topic_tags, sub_topic_tags
    except Exception as e:
        st.error(f"Error fetching Code Analysis topic/subtopic tags: {e}")
        return set(), set()

def is_valid_tag(tag_str, question_id=None):
    """
    Validates whether a string is a usable tag.
    Allows UUIDs only if they match the question_id.
    """
    tag_str = str(tag_str).strip()

    if not tag_str:
        return False

    skip_values = {
        'MULTIPLE_CHOICE', 'ENGLISH', 'MARKDOWN', 'TEXT', 'TRUE', 'FALSE'
    }
    if tag_str.upper() in skip_values:
        return False

    if tag_str.isdigit():
        return False

    if UUID_REGEX.match(tag_str):
        if question_id and tag_str == question_id:
            return True  # Allow if it's the same as the current question ID
        return False

    known_single_tags = {'NIAT', 'POOL_1', 'IN_OFFLINE_EXAM', 'IS_PUBLIC', 'IS_PRIVATE'}
    known_prefixes = ['COURSE_', 'MODULE_', 'UNIT_', 'SOURCE_', 'DIFFICULTY_', 'TOPIC_', 'SUB_TOPIC_']

    return (
        '_' in tag_str or
        tag_str in known_single_tags or
        any(tag_str.startswith(prefix) for prefix in known_prefixes)
    )

def extract_json_files(zip_file):
    """Extracts JSON files from a zip archive and loads question objects (supports list of questions at top level)."""
    import zipfile  # Ensure this is imported
    temp_dir = tempfile.mkdtemp()
    all_questions = []
    try:
        # Save the uploaded BytesIO object to a temporary file
        with open(os.path.join(temp_dir, zip_file.name), "wb") as f:
            f.write(zip_file.getvalue())

        zip_path = os.path.join(temp_dir, zip_file.name)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        for root, _, files in os.walk(temp_dir):
            for file in files:
                if file.endswith(".json"):
                    full_path = os.path.join(root, file)
                    try:
                        with open(full_path, 'r', encoding='utf-8') as f:
                            json_data = json.load(f)

                            if isinstance(json_data, list):
                                for q in json_data:
                                    if isinstance(q, dict):
                                        # 🔍 Extract question_id from input_output field
                                        input_output = q.get("input_output", [])
                                        if input_output and isinstance(input_output[0], dict):
                                            q["question_id"] = input_output[0].get("question_id", "Unknown")
                                        else:
                                            q["question_id"] = "Unknown"
                                        all_questions.append(q)

                    except json.JSONDecodeError as e:
                        st.warning(f"Failed to decode JSON from {file}: {e}")
                    except Exception as e:
                        st.warning(f"Failed to read {file}: {e}")
    finally:
        shutil.rmtree(temp_dir)
    return all_questions

def extract_mcq_data(uploaded_file):
    import io
    questions = []
    try:
        file_bytes = uploaded_file.getvalue()
        file_like_object = io.BytesIO(file_bytes)

        if uploaded_file.name.endswith('.xlsx'):
            df = pd.read_excel(file_like_object, sheet_name='Questions', header=None)
        elif uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(file_like_object, header=None)
        else:
            st.error(f"Unsupported file type: {uploaded_file.name}. Only .xlsx and .csv are supported for MCQ files.")
            return []
    except Exception as e:
        st.error(f"Error reading uploaded file {uploaded_file.name}: {e}")
        return []

    st.info(f"Processing {len(df)} rows from 'Questions' sheet...")

    current_question = None

    for i, row in df.iterrows():
        question_type = str(row[1]).strip().upper() if pd.notna(row[1]) else ""
        question_id = str(row[0]).strip() if pd.notna(row[0]) else ""

        # Start a new question when MULTIPLE_CHOICE and ID are present
        if question_type == "MULTIPLE_CHOICE" and question_id:
            if current_question:
                questions.append(current_question)

            current_question = {
                "question_id": question_id,
                "tag_names": set(),
                "type": "MCQ"
            }

        # Add tag to the current question
        if current_question and pd.notna(row[12]):
            tag = str(row[12]).strip()
            if '\n' in tag:
                for t in tag.split('\n'):
                    if is_valid_tag(t,question_id=current_question["question_id"]):
                        current_question["tag_names"].add(t.strip())
            else:
                if is_valid_tag(tag,question_id=current_question["question_id"]):
                    current_question["tag_names"].add(tag)

    if current_question:
        questions.append(current_question)

    st.success(f"Extracted {len(questions)} MCQ questions from 'Questions' sheet")
    return questions

topic_tags, sub_topic_tags = fetch_topic_subtopic_tags_code_analysis()

def fetch_and_parse_json_from_url():
    """
    Fetches raw JSON from the defined URL.
    Returns the parsed JSON dictionary or None if an error occurs.
    """
    try:
        response = requests.get(S3_TOPIC_URL)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching JSON: {e}")
        return None
    except json.JSONDecodeError as e: # If response is not valid JSON
        print(f"Error decoding JSON: {e}")
        return None

def get_processed_data(raw_data):
    """
    Processes the raw JSON data (specifically the 'question_tags' part)
    into the nested dictionary structure:
    {section_value: {topic_value: [sub_topic_value1, sub_topic_value2, ...]}}
    This data is used to populate the dropdowns with 'value' fields.
    """
    if not raw_data or "question_tags" not in raw_data:
        print("Warning: 'question_tags' not found in raw_data or raw_data is empty.")
        return {}

    question_tags_data = raw_data.get("question_tags", {})
    updated_data = {}
    
    # Iterate over sorted section keys (these are the 'values' for sections)
    for section_key in sorted(list(question_tags_data.keys())):
        updated_data[section_key] = {}
        
        section_items = question_tags_data.get(section_key, [])
        if not isinstance(section_items, list):
            continue

        for section_data_item in section_items:
            if not isinstance(section_data_item, dict):
                continue

            topic_name_data = section_data_item.get('topic_name', {})
            topic_value = topic_name_data.get('value')

            if not topic_value: # Skip if topic_value is missing or empty
                continue
            
            subtopics_values = []
            sub_topics_list = section_data_item.get('sub_topics', [])
            if not isinstance(sub_topics_list, list):
                continue

            for subtopic_data_item in sub_topics_list:
                if not isinstance(subtopic_data_item, dict):
                    continue
                
                sub_topic_name_data = subtopic_data_item.get('sub_topic_name', {})
                subtopic_value = sub_topic_name_data.get('value')
                if subtopic_value: # Add only if subtopic_value is not empty
                    subtopics_values.append(subtopic_value)
            
            # Store sorted list of subtopic values for the current topic
            updated_data[section_key][topic_value] = sorted(subtopics_values)
            
    return updated_data

# Load JSON and process valid topic/sub-topic tags
raw_json = fetch_and_parse_json_from_url()
processed_data = get_processed_data(raw_json)

valid_topic_tags_MCQS = set()
valid_sub_topic_tags_MCQS = set()

if "CODE_ANALYSIS" in processed_data:
    for topic_val, subtopics in processed_data["CODE_ANALYSIS"].items():
        valid_topic_tags_MCQS.add(topic_val)
        valid_sub_topic_tags_MCQS.update(subtopics)

# --- Validation logic ---
def validate_question_tags(question, module_type, unit_tag, course_tag, module_tag):
    """
    Validates tags for a single question dictionary.
    """
    qid = question.get("question_id", "Unknown")
    tag_names = set(question.get("tag_names", []))
    missing = []

    # Required tags
    for tag in REQUIRED_TAGS["COMMON"]:
        if tag not in tag_names:
            missing.append(tag)

    # Difficulty tag
    if not any(tag in tag_names for tag in REQUIRED_TAGS["DIFFICULTY"]):
        missing.append("One of: " + ", ".join(REQUIRED_TAGS["DIFFICULTY"]))

    # Source tag
    if not any(tag.startswith(REQUIRED_TAGS["SOURCE"]) for tag in tag_names):
        missing.append("SOURCE_* (any tag starting with SOURCE_)")

    # Question ID tag
    if qid not in tag_names:
        missing.append(f"Question ID tag: {qid}")

    # Conditional tag logic
    if module_type in {"MCQ", "Code Analysis"}:
        if "IS_PUBLIC" not in tag_names:
            missing.append("IS_PUBLIC")
        if "IS_PRIVATE" in tag_names:
            missing.append("IS_PRIVATE (should not be present)")
    elif module_type in {"Python Coding", "Web Coding"}:
        if "IS_PRIVATE" not in tag_names:
            missing.append("IS_PRIVATE")
        if "IS_PUBLIC" in tag_names:
            missing.append("IS_PUBLIC (should not be present)")

    # Course/module/unit
    if course_tag and course_tag not in tag_names:
        missing.append(f"Course tag: {course_tag}")
    if module_tag and module_tag not in tag_names:
        missing.append(f"Module tag: {module_tag}")
    if unit_tag and unit_tag not in tag_names:
        missing.append(f"Unit tag: {unit_tag}")
    if module_type in {"MCQ", "Code Analysis"}:
        for tag in tag_names:
            if tag.startswith("TOPIC_") and tag not in valid_topic_tags_MCQS:
                missing.append(f"Invalid TOPIC tag: {tag}")
            if tag.startswith("SUB_TOPIC_") and tag not in valid_sub_topic_tags_MCQS:
                missing.append(f"Invalid SUB_TOPIC tag: {tag}")

    return qid, missing

# --- Streamlit UI ---
st.title("📦 Question Tag Validator App")
st.markdown("Upload your question files (Excel for MCQs, ZIP for JSONs) to check for **EXACT** tag compliance.")
st.markdown("---")

# Initialize session state for debug mode
if 'debug_mode' not in st.session_state:
    st.session_state.debug_mode = False

# Add debug mode
st.session_state.debug_mode = st.checkbox("🔍 Debug Mode (show detailed processing info)", value=st.session_state.debug_mode)
st.markdown("---")

st.header("1. Enter Required Tags")
st.markdown("*Enter the tag names below and press Enter to auto-format them with proper prefixes and formatting.*")

col1, col2, col3 = st.columns(3)
with col1:
    course_input = st.text_input(
        "Course Name", 
        key="course_raw", 
        help="Enter the course name (e.g., 'Python', 'React JS'). Will be formatted as COURSE_Python, COURSE_React_JS",
        placeholder="e.g., Python"
    )
    if course_input:
        formatted_course = format_tag_name(course_input, "COURSE_")
        st.session_state.formatted_course_tag = formatted_course
        if formatted_course:
            st.success(f"✅ Formatted: **{formatted_course}**")

with col2:
    module_input = st.text_input(
        "Module Name", 
        key="module_raw", 
        help="Enter the module name (e.g., 'Looping', 'State Management'). Will be formatted as MODULE_LOOPING, MODULE_STATE_MANAGEMENT",
        placeholder="e.g., Looping"
    )
    if module_input:
        formatted_module = format_tag_name(module_input, "MODULE_")
        st.session_state.formatted_module_tag = formatted_module
        if formatted_module:
            st.success(f"✅ Formatted: **{formatted_module}**")

with col3:
    unit_input = st.text_input(
        "Unit Name", 
        key="unit_raw", 
        help="Enter the unit name (e.g., 'Nested Conditions', 'API Integration'). Will be formatted as UNIT_NESTED_CONDITIONS, UNIT_API_INTEGRATION",
        placeholder="e.g., Nested Conditions"
    )
    if unit_input:
        formatted_unit = format_tag_name(unit_input, "UNIT_")
        st.session_state.formatted_unit_tag = formatted_unit
        if formatted_unit:
            st.success(f"✅ Formatted: **{formatted_unit}**")

st.markdown("---")
st.header("2. Upload Question Files")
mcq_file = st.file_uploader("📄 Upload MCQ Excel file (.xlsx or .csv)", type=["xlsx", "csv"], help="Upload your Excel or CSV file containing MCQ questions. See 'Expected Excel Format' below for details.")
json_zip_file = st.file_uploader("📁 Upload JSON zip file (for Coding/Code Analysis questions)", type=["zip"], help="Upload a ZIP archive containing JSON files for Python Coding, Web Coding, or Code Analysis questions.")
st.markdown("---")

st.header("3. Run Validation")
if st.button("🚀 Run Tag Check", type="primary"):
    if not (st.session_state.formatted_course_tag and st.session_state.formatted_module_tag and st.session_state.formatted_unit_tag):
        st.warning("Please provide Course, Module, and Unit names in Section 1 and ensure they are properly formatted.")
    elif not (mcq_file or json_zip_file):
        st.warning("Please upload at least one file in Section 2: an MCQ sheet or a JSON ZIP.")
    else:
        with st.spinner("Fetching unit-subtopic mapping and processing files... This might take a moment."):
            # Use the formatted tags for validation
            course_tag = st.session_state.formatted_course_tag
            module_tag = st.session_state.formatted_module_tag
            unit_tag = st.session_state.formatted_unit_tag
            
            # Display the tags being used for validation
            st.info(f"Using tags: **{course_tag}**, **{module_tag}**, **{unit_tag}**")
            
            # unit_map is still fetched, but its subtopic data is not used for validation, only for the unit_tag check itself.
            unit_map = fetch_unit_subtopic_map() 
            
            all_questions = []
            validation_details = []

            if mcq_file:
                st.subheader("📊 Processing MCQ Excel/CSV File...")
                mcqs = extract_mcq_data(mcq_file)
                st.info(f"Found {len(mcqs)} MCQ questions. Now validating tags...")
                
                for q in mcqs:
                    qid, missing = validate_question_tags(q, "MCQ", unit_tag, course_tag, module_tag)
                    
                    validation_details.append({
                        "question_id": qid,
                        "module_type": "MCQ",
                        "current_tags": sorted(list(q.get("tag_names", []))), # Convert set to list for display
                        "missing_tags": missing,
                        "has_issues": len(missing) > 0
                    })
                    
                    if missing:
                        all_questions.append({
                            "question_id": qid, 
                            "module_type": "MCQ", 
                            "missing_tags": ", ".join(missing),
                            "current_tags": ", ".join(sorted(list(q.get("tag_names", []))))
                        })

            if json_zip_file:
                st.subheader("📁 Processing JSON ZIP File...")
                jsons = extract_json_files(json_zip_file)
                st.info(f"Found {len(jsons)} JSON questions. Now validating tags...")
                
                for q in jsons:
                    qtype = q.get("question_type", "")
                    module_type = "Unknown"
                    if qtype == "CODE_ANALYSIS_MULTIPLE_CHOICE":
                        module_type = "Code Analysis"
                    elif qtype == "CODING":
                        if q.get("question_format") == "CODING_PRACTICE":
                            module_type = "Python Coding"
                        elif q.get("question_format") == "WEB_CODING":
                            module_type = "Web Coding"
                        else:
                            # Default to Python Coding if format is unclear but type is CODING
                            module_type = "Python Coding" 
                    
                    qid, missing = validate_question_tags(q, module_type, unit_tag, course_tag, module_tag)
                    
                    validation_details.append({
                        "question_id": qid,
                        "module_type": module_type,
                        "current_tags": sorted(list(q.get("tag_names", []))), # Convert set to list for display
                        "missing_tags": missing,
                        "has_issues": len(missing) > 0
                    })
                    
                    if missing:
                        all_questions.append({
                            "question_id": qid, 
                            "module_type": module_type, 
                            "missing_tags": ", ".join(missing),
                            "current_tags": ", ".join(sorted(list(q.get("tag_names", []))))
                        })

        st.markdown("---")
        st.header("4. Validation Results")

        # Show detailed validation results if debug mode is on
        if st.session_state.debug_mode and validation_details:
            st.subheader("🔍 Detailed Validation Results (first 10 with issues)")
            issues_found_count = 0
            for detail in validation_details:
                if detail['has_issues']:
                    if issues_found_count < 10: # Limit displayed details for brevity
                        with st.expander(f"Question: {detail['question_id']} ({'❌ Has Issues' if detail['has_issues'] else '✅ OK'})"):
                            st.write(f"**Module Type:** {detail['module_type']}")
                            st.write(f"**Current Tags:** {', '.join(detail['current_tags'])}")
                            if detail['missing_tags']:
                                st.write(f"**Missing Tags:** {', '.join(detail['missing_tags'])}")
                        issues_found_count += 1
                    else:
                        st.info(f"Showing only the first 10 questions with issues. Total issues found: {len([d for d in validation_details if d['has_issues']])}")
                        break # Stop displaying after 10
            if issues_found_count == 0 and [d for d in validation_details if d['has_issues']]: # If issues exist but none shown due to limit
                st.info("No detailed issues displayed (all questions are valid or debug limit reached).")
            elif not [d for d in validation_details if d['has_issues']]: # If no issues at all
                st.info("No detailed issues to display (all questions are valid).")

        # Display final results table and summary
        if all_questions:
            result_df = pd.DataFrame(all_questions)
            st.subheader("❌ Questions with Missing Tags")
            st.dataframe(result_df, use_container_width=True)
            
            # Summary statistics
            st.subheader("📈 Summary")
            total_questions = len(validation_details)
            issues_count = len(all_questions)
            
            col_summary1, col_summary2, col_summary3 = st.columns(3)
            with col_summary1:
                st.metric("Total Questions Processed", total_questions)
            with col_summary2:
                st.metric("Questions with Issues", issues_count)
            with col_summary3:
                if total_questions > 0:
                    st.metric("Success Rate", f"{((total_questions - issues_count) / total_questions * 100):.1f}%")
                else:
                    st.metric("Success Rate", "N/A") # Avoid division by zero
            
            # Download button
            csv_data = result_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "⬇️ Download Report as CSV", 
                csv_data, 
                "missing_tags_report.csv",
                mime="text/csv"
            )
        else:
            st.success("✅ All questions are properly tagged! No missing tags found.")

st.markdown("---")
# Add sample format information
with st.expander("📋 Expected File Formats & Required Tags"):
    st.markdown("""
    **Tag Formatting Rules:**
    - Course/Module/Unit names are automatically formatted with proper prefixes
    - All text is converted to uppercase
    - Spaces and special characters are replaced with underscores
    - Multiple consecutive underscores are collapsed to single underscores
    - Examples:
        - Input: "Python" → Output: "COURSE_PYTHON"
        - Input: "React JS" → Output: "COURSE_REACT_JS"
        - Input: "State Management" → Output: "MODULE_STATE_MANAGEMENT"
        - Input: "Nested Conditions" → Output: "UNIT_NESTED_CONDITIONS"
    
    **Expected Excel/CSV Format for MCQ Questions:**
    - The file should contain a row for each question, typically with the `question_id` in the **first column (Column A)** and `MULTIPLE_CHOICE` in the **second column (Column B)**.
    - Tags associated with a question should be present in the **thirteenth column (Column M, index 12)**.
    - Tags can be on the same row as the `MULTIPLE_CHOICE` entry, or in subsequent rows directly below it within Column M.
    - Multiple tags within a single cell in Column M can be separated by newlines.

    **Expected JSON Format (for ZIP files):**
    - Each JSON file should represent a question.
    - It should contain a `question_id` field and a `tag_names` array (list of strings).
    - The `question_type` field (e.g., `CODE_ANALYSIS_MULTIPLE_CHOICE`, `CODING`) is used to determine `IS_PUBLIC` or `IS_PRIVATE` tag requirements.
    - For `CODING` type, `question_format` (e.g., `CODING_PRACTICE` for Python, `WEB_CODING` for Web) helps categorize.

    **Required Tags (EXACT MATCH, case-sensitive):**
    - `NIAT`
    - `IN_OFFLINE_EXAM`
    - `POOL_1`
    - One of: `DIFFICULTY_EASY`, `DIFFICULTY_MEDIUM`, `DIFFICULTY_HARD`
    - Any tag starting with `SOURCE_` (e.g., `SOURCE_GPT`, `SOURCE_BOOK`)
    - The **Question ID (UUID)** itself, must be present as one of the tags.
    - **Conditional Tags based on Question Type:**
        - `IS_PUBLIC` (for MCQ and Code Analysis questions)
        - `IS_PRIVATE` (for Python Coding and Web Coding questions)
        - **Important**: If `IS_PUBLIC` is found for a coding question (Python/Web), it will be flagged as an issue.
        - **Important**: If `IS_PRIVATE` is found for an MCQ/Code Analysis question, it will be flagged as an issue.
    - The exact `COURSE_` tag you provide in the input field (e.g., `COURSE_PYTHON`)
    - The exact `MODULE_` tag you provide in the input field (e.g., `MODULE_LOOPING`)
    - The exact `UNIT_` tag you provide in the input field (e.g., `UNIT_NESTED_CONDITIONS`)
    """)