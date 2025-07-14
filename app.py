import streamlit as st
import pandas as pd
import json
import requests
import re
import os
import tempfile
import shutil
import zipfile
import io

# --- Page Configuration (MUST be the first Streamlit command) ---
st.set_page_config(layout="wide", page_title="Question Tag Validator")

# --- Constants ---
S3_TOPIC_URL = "https://nxtwave-assessments-backend-nxtwave-media-static.s3.ap-south-1.amazonaws.com/topin_config_prod/static/static_content.json"
REQUIRED_TAGS = {
    "COMMON": ["NIAT", "IN_OFFLINE_EXAM", "POOL_1"],
    "DIFFICULTY": ["DIFFICULTY_EASY", "DIFFICULTY_MEDIUM", "DIFFICULTY_HARD"],
    "SOURCE": "SOURCE_",
}
UUID_REGEX = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$')

# Maps our internal module_type names to the actual keys in the S3 JSON file.
S3_KEY_MAPPING = {
    "MCQ": "CODE_ANALYSIS",
    "Code Analysis": "CODE_ANALYSIS",
    "Python Coding": "CODING",
    "Coding": "CODING",
    "JS Coding": "CODING",
    "DSA Coding": "CODING",
    "Web Coding": "HTML_CODING",
    "SQL Coding": "SQL_CODING",
}

# --- Helper Functions (Functionality Unchanged) ---

def format_tag_name(tag_input, prefix):
    if not tag_input or not tag_input.strip():
        return ""
    tag_input = tag_input.strip()
    if tag_input.startswith(prefix):
        tag_input = tag_input[len(prefix):]
    formatted_tag = re.sub(r'[^a-zA-Z0-9_]', '_', tag_input)
    formatted_tag = re.sub(r'_+', '_', formatted_tag)
    formatted_tag = formatted_tag.strip('_')
    return f"{prefix}{formatted_tag}" if formatted_tag else ""

@st.cache_data
def fetch_and_parse_all_tags():
    """
    Fetches the entire S3 config file and parses all topic/sub-topic tags,
    organizing them by their top-level S3 key (e.g., 'CODE_ANALYSIS', 'CODING').
    The result is cached to avoid repeated downloads.
    """
    all_tags_by_module = {}
    try:
        response = requests.get(S3_TOPIC_URL)
        response.raise_for_status()
        data = response.json()
        modules_container = data.get("question_tags", {})
        if not modules_container:
            st.error("FATAL: 'question_tags' key not found in the S3 JSON file. The structure may have changed.")
            return {}

        for s3_key, topics_list in modules_container.items():
            if not isinstance(topics_list, list):
                st.warning(f"Warning: Expected a list for module '{s3_key}', but found {type(topics_list)}. Skipping.")
                continue

            topic_tags, sub_topic_tags = set(), set()
            for topic in topics_list:
                if topic_val := topic.get("topic_name", {}).get("value"):
                    topic_tags.add(topic_val)
                for sub in topic.get("sub_topics", []):
                    if sub_val := sub.get("sub_topic_name", {}).get("value"):
                        sub_topic_tags.add(sub_val)
            all_tags_by_module[s3_key] = (topic_tags, sub_topic_tags)

        if not all_tags_by_module:
            st.error("FATAL: Parsing logic failed to extract any topic tags from the S3 file after finding 'question_tags'.")
            return {}
        return all_tags_by_module

    except requests.exceptions.RequestException as e:
        st.error(f"FATAL: Network error. Could not fetch the S3 configuration file: {e}")
        return {}
    except json.JSONDecodeError as e:
        st.error(f"FATAL: The S3 file is not valid JSON. Could not parse tags: {e}")
        return {}
    except Exception as e:
        st.error(f"FATAL: An unexpected error occurred while processing S3 topic configuration.")
        st.exception(e)
        return {}

def is_valid_tag(tag_str, question_id=None):
    tag_str = str(tag_str).strip()
    if not tag_str or tag_str.upper() in {'MULTIPLE_CHOICE', 'ENGLISH', 'MARKDOWN', 'TEXT', 'TRUE', 'FALSE'} or tag_str.isdigit():
        return False
    if UUID_REGEX.match(tag_str):
        return not question_id or tag_str == question_id
    known_single_tags = {'NIAT', 'POOL_1', 'IN_OFFLINE_EXAM', 'IS_PUBLIC', 'IS_PRIVATE'}
    known_prefixes = ['COURSE_', 'MODULE_', 'UNIT_', 'SOURCE_', 'DIFFICULTY_', 'TOPIC_', 'SUB_TOPIC_', 'COMPANY_']
    return '_' in tag_str or tag_str in known_single_tags or any(tag_str.startswith(prefix) for prefix in known_prefixes)

# --- File Processing Functions (Functionality Unchanged) ---

def extract_json_files(zip_file):
    temp_dir = tempfile.mkdtemp()
    all_questions = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_file.getvalue()), 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        for root, _, files in os.walk(temp_dir):
            if not files: continue

            folder_name = os.path.basename(root)
            module_type = "Unknown JSON Type"
            if "Code Analysis MCQs" in folder_name: module_type = "Code Analysis"
            elif "Coding Questions" in folder_name: module_type = "Python Coding"
            elif "SQL_Coding" in folder_name: module_type = "SQL Coding"
            elif "HTML_Code Questions" in folder_name: module_type = "Web Coding"

            for file in files:
                if file.endswith(".json"):
                    full_path = os.path.join(root, file)
                    try:
                        with open(full_path, 'r', encoding='utf-8') as f:
                            json_data = json.load(f)
                            if isinstance(json_data, dict): json_data = [json_data]

                            for q in json_data:
                                if not isinstance(q, dict): continue
                                tag_names = set(q.get("tag_names", []))
                                if module_type == "Code Analysis":
                                    question_id = q.get("input_output", [{}])[0].get("question_id")
                                else:
                                    question_id = q.get("question_id")
                                valid_tags = {tag for tag in tag_names if is_valid_tag(tag, question_id)}
                                all_questions.append({
                                    "question_id": question_id or f"Unknown_ID_in_{file}",
                                    "tag_names": valid_tags,
                                    "module_type": module_type,
                                    "original_data": q
                                })
                    except (json.JSONDecodeError, Exception) as e:
                        st.warning(f"Failed to process {file} in folder '{folder_name}': {e}")
    finally:
        shutil.rmtree(temp_dir)
    return all_questions

def extract_mcq_data(uploaded_file):
    questions = []
    try:
        df = pd.read_excel(uploaded_file, sheet_name='Questions', header=None) if uploaded_file.name.endswith('.xlsx') else pd.read_csv(uploaded_file, header=None)
    except Exception as e:
        st.error(f"Error reading {uploaded_file.name}: {e}")
        return []

    current_question = None
    for _, row in df.iterrows():
        q_type = str(row[1]).strip().upper() if pd.notna(row[1]) else ""
        q_id = str(row[0]).strip() if pd.notna(row[0]) else ""

        if q_type == "MULTIPLE_CHOICE" and q_id:
            if current_question: questions.append(current_question)
            current_question = {"question_id": q_id, "tag_names": set(), "module_type": "MCQ"}

        if current_question and pd.notna(row[12]):
            for tag in str(row[12]).strip().split('\n'):
                if cleaned_tag := tag.strip():
                    if is_valid_tag(cleaned_tag, question_id=current_question["question_id"]):
                        current_question["tag_names"].add(cleaned_tag)

    if current_question: questions.append(current_question)
    return questions

# --- Validation Function (Functionality Unchanged) ---

def validate_question_tags(question, module_type, unit_tags, course_tag, module_tag, company_tag, valid_topic_tags, valid_sub_topic_tags):
    qid = question.get("question_id", "Unknown")
    tag_names = set(question.get("tag_names", []))
    issues = []

    # 1. Check for MISSING required tags
    for tag in REQUIRED_TAGS["COMMON"]:
        if tag not in tag_names: issues.append(f"Missing: {tag}")
    if not any(tag in tag_names for tag in REQUIRED_TAGS["DIFFICULTY"]): issues.append(f"Missing: One of {', '.join(REQUIRED_TAGS['DIFFICULTY'])}")
    if not any(tag.startswith(REQUIRED_TAGS["SOURCE"]) for tag in tag_names): issues.append("Missing: SOURCE_* tag")
    if qid not in tag_names: issues.append(f"Missing: Question ID tag ({qid})")

    # 2. Check for INVALID tags for the given context
    if module_type in {"MCQ", "Code Analysis"}:
        if "IS_PUBLIC" not in tag_names: issues.append("Missing: IS_PUBLIC")
        if "IS_PRIVATE" in tag_names: issues.append("Invalid: IS_PRIVATE (should not be on this type)")
    elif module_type in {"Python Coding", "Web Coding", "SQL Coding", "Coding"}:
        if "IS_PRIVATE" not in tag_names: issues.append("Missing: IS_PRIVATE")
        if "IS_PUBLIC" in tag_names: issues.append("Invalid: IS_PUBLIC (should not be on this type)")

    for tag in tag_names:
        if tag.startswith("TOPIC_") and tag not in valid_topic_tags: issues.append(f"Invalid TOPIC_ tag: {tag}")
        if tag.startswith("SUB_TOPIC_") and tag not in valid_sub_topic_tags: issues.append(f"Invalid SUB_TOPIC_ tag: {tag}")

    # 3. Check for MISSING optional tags (if specified)
    if course_tag and course_tag not in tag_names: issues.append(f"Missing: {course_tag}")
    if module_tag and module_tag not in tag_names: issues.append(f"Missing: {module_tag}")
    if unit_tags and not any(unit in tag_names for unit in unit_tags): issues.append(f"Missing: One of {', '.join(unit_tags)}")
    if company_tag and company_tag not in tag_names: issues.append(f"Missing: {company_tag}")

    # 4. Check for FOUND optional tags (if NOT specified)
    if not course_tag and any(t.startswith("COURSE_") for t in tag_names): issues.append(f"Found Optional: {next(t for t in tag_names if t.startswith('COURSE_'))}")
    if not module_tag and any(t.startswith("MODULE_") for t in tag_names): issues.append(f"Found Optional: {next(t for t in tag_names if t.startswith('MODULE_'))}")
    if not unit_tags and any(t.startswith("UNIT_") for t in tag_names):
        for tag in tag_names:
            if tag.startswith("UNIT_"): issues.append(f"Found Optional: {tag}")
    if not company_tag and any(t.startswith("COMPANY_") for t in tag_names): issues.append(f"Found Optional: {next(t for t in tag_names if t.startswith('COMPANY_'))}")

    return qid, issues

# --- Initialize Session State ---
for key in ['formatted_course_tag', 'formatted_module_tag', 'formatted_unit_tag', 'extra_unit_tag', 'formatted_company_tag']:
    if key not in st.session_state: st.session_state[key] = ""
if 'debug_mode' not in st.session_state: st.session_state['debug_mode'] = False
if 'validation_run' not in st.session_state: st.session_state['validation_run'] = False


# --- ============================= ---
# ---     NEW STREAMLIT UI LAYOUT     ---
# --- ============================= ---

# --- Sidebar ---
with st.sidebar:
    st.title("⚙️ Settings & Guide")
    st.session_state.debug_mode = st.checkbox(
        "🔍 Enable Debug Mode",
        value=st.session_state.debug_mode,
        help="If checked, the results area will show a detailed log for every single question processed, not just the ones with errors."
    )
    st.markdown("---")

    with st.expander("📚 Tagging Rules & File Formats", expanded=True):
        st.markdown("""
        #### Required Tags (MUST be present)
        - `NIAT`, `IN_OFFLINE_EXAM`, `POOL_1`
        - One of: `DIFFICULTY_EASY`, `DIFFICULTY_MEDIUM`, `DIFFICULTY_HARD`
        - A `SOURCE_*` tag (e.g., `SOURCE_GPT`)
        - The Question ID (UUID) itself.
        - **Conditional:** `IS_PUBLIC` (for MCQ/Code Analysis) or `IS_PRIVATE` (for Coding).

        #### Optional Tags (Validation Behavior)
        - This tool checks for `COURSE_`, `MODULE_`, `UNIT_`, and `COMPANY_` tags.
        - **If you enter a tag name in Step 1:** The tool will report it as **`Missing`** if it's not found.
        - **If you leave a field in Step 1 blank:** The tool will report it as **`Found Optional`** if it finds a corresponding tag.

        #### Invalid Tags
        - A tag is **`Invalid`** if it's wrong for the context:
            - `IS_PUBLIC` on a Coding question.
            - A `TOPIC_` or `SUB_TOPIC_` tag not in the official list for that module type.

        #### Supported ZIP File Structure
        The question type is determined by the **folder name** inside the ZIP file:
        - `.../Code Analysis MCQs/...` ➔ **Code Analysis**
        - `.../Coding Questions/...` ➔ **Python Coding**
        - `.../SQL_Coding/...` ➔ **SQL Coding**
        - `.../HTML_Code Questions/...` ➔ **Web Coding**
        """)

# --- Main Page ---
st.title("📦 Question Tag Validator")
st.markdown("A tool to check for **missing**, **invalid**, or **unexpected** tags in your question files. Follow the steps below.")
st.markdown("---")


# --- Step 1: Configuration ---
st.header("1. Configure Optional Tags")
st.info("Enter tag names below to check for their presence. If a field is blank, the tool will instead report if it finds any unexpected tags of that type.")

with st.container(border=True):
    col1, col2 = st.columns(2)
    with col1:
        course_input = st.text_input("Course Name", key="course_raw", placeholder="e.g., Python")
        st.session_state.formatted_course_tag = format_tag_name(course_input, "COURSE_")
        if st.session_state.formatted_course_tag:
            st.success(f"Checks for: `{st.session_state.formatted_course_tag}`")

        unit_input = st.text_input("Unit Name", key="unit_raw", placeholder="e.g., Nested Conditions")
        st.session_state.formatted_unit_tag = format_tag_name(unit_input, "UNIT_")
        if st.session_state.formatted_unit_tag:
            st.success(f"Checks for: `{st.session_state.formatted_unit_tag}`")

    with col2:
        module_input = st.text_input("Module Name", key="module_raw", placeholder="e.g., Looping")
        st.session_state.formatted_module_tag = format_tag_name(module_input, "MODULE_")
        if st.session_state.formatted_module_tag:
            st.success(f"Checks for: `{st.session_state.formatted_module_tag}`")

        extra_unit_input = st.text_input("Additional Unit Name", key="extra_unit_raw", placeholder="e.g., Loops")
        st.session_state.extra_unit_tag = format_tag_name(extra_unit_input, "UNIT_")
        if st.session_state.extra_unit_tag:
            st.success(f"Checks for: `{st.session_state.extra_unit_tag}`")

    company_input = st.text_input("Company Name", key="company_raw", placeholder="e.g., TCS")
    st.session_state.formatted_company_tag = format_tag_name(company_input, "COMPANY_")
    if st.session_state.formatted_company_tag:
        st.success(f"Checks for: `{st.session_state.formatted_company_tag}`")

# --- Step 2: Upload Files ---
st.header("2. Upload Files")
with st.container(border=True):
    ucol1, ucol2 = st.columns(2)
    with ucol1:
        mcq_file = st.file_uploader("📄 Upload MCQ Excel/CSV File", type=["xlsx", "csv"], help="Upload the Excel or CSV file containing MCQ questions.")
    with ucol2:
        json_zip_file = st.file_uploader("📁 Upload JSON ZIP File", type=["zip"], help="Upload a ZIP file containing JSONs for Coding, Code Analysis, SQL, or Web questions.")

# --- Step 3: Run Validation ---
st.markdown("---")
if st.button("🚀 Run Tag Check", type="primary", use_container_width=True):
    st.session_state.validation_run = True
    if not (mcq_file or json_zip_file):
        st.warning("Please upload at least one file in Step 2 to start the validation.")
        st.stop()

    with st.spinner("Hold on... fetching S3 tags, processing files, and validating questions..."):
        course_tag = st.session_state.formatted_course_tag
        module_tag = st.session_state.formatted_module_tag
        company_tag = st.session_state.formatted_company_tag
        unit_tags = [t for t in [st.session_state.formatted_unit_tag, st.session_state.extra_unit_tag] if t]
        all_valid_tags = fetch_and_parse_all_tags()

        if not all_valid_tags:
            st.error("Could not proceed with validation as topic/sub-topic tags failed to load from S3.")
            st.stop()

        all_questions_with_issues = []
        validation_details = []

        def process_questions(questions, file_type_name):
            if not questions: return
            for q in questions:
                module_type = q.get("module_type", "Unknown")
                s3_key = S3_KEY_MAPPING.get(module_type)
                valid_topic_tags, valid_sub_topic_tags = all_valid_tags.get(s3_key, (set(), set()))
                qid, issues = validate_question_tags(q, module_type, unit_tags, course_tag, module_tag, company_tag, valid_topic_tags, valid_sub_topic_tags)
                has_issues = len(issues) > 0
                detail_entry = {"question_id": qid, "module_type": module_type, "current_tags": sorted(list(q.get("tag_names", []))), "issues": issues, "has_issues": has_issues}
                validation_details.append(detail_entry)
                if has_issues:
                    all_questions_with_issues.append({"Question ID": qid, "Module Type": module_type, "Issues Found": ", ".join(issues), "Current Tags": ", ".join(detail_entry['current_tags'])})

        if mcq_file: process_questions(extract_mcq_data(mcq_file), "MCQ")
        if json_zip_file: process_questions(extract_json_files(json_zip_file), "JSON")

        st.session_state.validation_details = validation_details
        st.session_state.all_questions_with_issues = all_questions_with_issues

# --- Step 4: Display Results ---
if st.session_state.validation_run:
    st.header("4. Validation Results")
    validation_details = st.session_state.get('validation_details', [])
    all_questions_with_issues = st.session_state.get('all_questions_with_issues', [])

    if not validation_details:
        st.warning("No questions were extracted from the uploaded files. Please check the file contents and formats.")
    else:
        # --- Summary Metrics ---
        with st.container(border=True):
            st.subheader("📊 Summary")
            total_q = len(validation_details)
            issues_q = len(all_questions_with_issues)
            success_q = total_q - issues_q
            success_rate = (success_q / total_q * 100) if total_q > 0 else 0

            c1, c2, c3 = st.columns(3)
            c1.metric("Total Questions Processed", total_q)
            c2.metric("✅ Questions without Issues", success_q)
            c3.metric("❌ Questions with Issues", issues_q)

            if success_rate == 100:
                st.progress(100, text=f"Success Rate: {success_rate:.1f}%")
                st.success("🎉 Fantastic! All questions are properly tagged. No issues found.")
                st.balloons()
            else:
                st.progress(int(success_rate), text=f"Success Rate: {success_rate:.1f}%")

        # --- Table of Issues ---
        if all_questions_with_issues:
            st.subheader("🚨 Issues Report")
            result_df = pd.DataFrame(all_questions_with_issues)
            st.dataframe(result_df, use_container_width=True, hide_index=True)
            st.download_button("⬇️ Download Report as CSV", result_df.to_csv(index=False).encode('utf-8'), "tag_issues_report.csv", "text/csv")

        # --- Detailed Debug Log ---
        if st.session_state.debug_mode:
            st.subheader("🔍 Detailed Validation Log")
            with st.container(border=True):
                for detail in validation_details:
                    status = '❌ Has Issues' if detail['has_issues'] else '✅ OK'
                    with st.expander(f"Question: `{detail['question_id']}` ({detail['module_type']}) - {status}"):
                        st.write(f"**Current Tags:** `{', '.join(detail['current_tags'])}`")
                        if detail['issues']:
                            st.error(f"**Issues:** {'; '.join(detail['issues'])}")
                        else:
                            st.success("No issues found for this question.")