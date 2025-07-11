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

st.set_page_config(layout="wide", page_title="Question Tag Validator")

# --- Constants ---
S3_TOPIC_URL = "https://nxtwave-assessments-backend-nxtwave-media-static.s3.ap-south-1.amazonaws.com/topin_config_prod/static/static_content.json"
REQUIRED_TAGS = {
    "COMMON": ["NIAT", "IN_OFFLINE_EXAM", "POOL_1"],
    "DIFFICULTY": ["DIFFICULTY_EASY", "DIFFICULTY_MEDIUM", "DIFFICULTY_HARD"],
    "SOURCE": "SOURCE_",
}
UUID_REGEX = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$')

# --- Helper Functions ---

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
def fetch_topic_subtopic_tags_code_analysis():
    try:
        response = requests.get(S3_TOPIC_URL)
        response.raise_for_status()
        data = response.json()
        topic_tags, sub_topic_tags = set(), set()
        code_analysis_data = data.get("CODE_ANALYSIS", [])
        for topic in code_analysis_data:
            if topic_val := topic.get("topic_name", {}).get("value"):
                topic_tags.add(topic_val)
            for sub in topic.get("sub_topics", []):
                if sub_val := sub.get("sub_topic_name", {}).get("value"):
                    sub_topic_tags.add(sub_val)
        return topic_tags, sub_topic_tags
    except Exception as e:
        st.error(f"Error fetching Code Analysis tags: {e}")
        return set(), set()

def is_valid_tag(tag_str, question_id=None):
    tag_str = str(tag_str).strip()
    if not tag_str or tag_str.upper() in {'MULTIPLE_CHOICE', 'ENGLISH', 'MARKDOWN', 'TEXT', 'TRUE', 'FALSE'} or tag_str.isdigit():
        return False
    if UUID_REGEX.match(tag_str):
        return not question_id or tag_str == question_id
    known_single_tags = {'NIAT', 'POOL_1', 'IN_OFFLINE_EXAM', 'IS_PUBLIC', 'IS_PRIVATE'}
    known_prefixes = ['COURSE_', 'MODULE_', 'UNIT_', 'SOURCE_', 'DIFFICULTY_', 'TOPIC_', 'SUB_TOPIC_', 'COMPANY_']
    return '_' in tag_str or tag_str in known_single_tags or any(tag_str.startswith(prefix) for prefix in known_prefixes)


# --- File Processing Functions ---
def extract_json_files(zip_file):
    temp_dir = tempfile.mkdtemp()
    all_questions = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_file.getvalue()), 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        for root, _, files in os.walk(temp_dir):
            for file in files:
                if file.endswith(".json"):
                    full_path = os.path.join(root, file)
                    try:
                        with open(full_path, 'r', encoding='utf-8') as f:
                            json_data = json.load(f)
                            if isinstance(json_data, dict):
                                json_data = [json_data]
                            for q in json_data:
                                if isinstance(q, dict):
                                    q["question_id"] = q.get("question_id", "Unknown_ID")
                                    all_questions.append(q)
                    except (json.JSONDecodeError, Exception) as e:
                        st.warning(f"Failed to process {file}: {e}")
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
            current_question = {"question_id": q_id, "tag_names": set(), "type": "MCQ"}

        if current_question and pd.notna(row[12]):
            for tag in str(row[12]).strip().split('\n'):
                if cleaned_tag := tag.strip():
                    if is_valid_tag(cleaned_tag, question_id=current_question["question_id"]):
                        current_question["tag_names"].add(cleaned_tag)
    
    if current_question: questions.append(current_question)
    return questions

# Load valid tags from S3
valid_topic_tags, valid_sub_topic_tags = fetch_topic_subtopic_tags_code_analysis()

# <<< CHANGE 1: RESTRUCTURED VALIDATION FUNCTION TO RETURN A SINGLE `issues` LIST >>>
def validate_question_tags(question, module_type, unit_tags, course_tag, module_tag, company_tag):
    qid = question.get("question_id", "Unknown")
    tag_names = set(question.get("tag_names", []))
    issues = []

    # --- 1. Check for MISSING required tags ---
    for tag in REQUIRED_TAGS["COMMON"]:
        if tag not in tag_names:
            issues.append(f"Missing: {tag}")
    if not any(tag in tag_names for tag in REQUIRED_TAGS["DIFFICULTY"]):
        issues.append(f"Missing: One of {', '.join(REQUIRED_TAGS['DIFFICULTY'])}")
    if not any(tag.startswith(REQUIRED_TAGS["SOURCE"]) for tag in tag_names):
        issues.append("Missing: SOURCE_* tag")
    if qid not in tag_names:
        issues.append(f"Missing: Question ID tag ({qid})")

    # --- 2. Check for INVALID tags for the given context ---
    if module_type in {"MCQ", "Code Analysis"}:
        if "IS_PUBLIC" not in tag_names: issues.append("Missing: IS_PUBLIC")
        if "IS_PRIVATE" in tag_names: issues.append("Invalid: IS_PRIVATE (should not be on this type)")
    elif module_type in {"Python Coding", "Web Coding"}:
        if "IS_PRIVATE" not in tag_names: issues.append("Missing: IS_PRIVATE")
        if "IS_PUBLIC" in tag_names: issues.append("Invalid: IS_PUBLIC (should not be on this type)")
    
    for tag in tag_names:
        if tag.startswith("TOPIC_") and tag not in valid_topic_tags:
            issues.append(f"Invalid TOPIC_ tag: {tag}")
        if tag.startswith("SUB_TOPIC_") and tag not in valid_sub_topic_tags:
            issues.append(f"Invalid SUB_TOPIC_ tag: {tag}")

    # --- 3. Check for MISSING optional tags (if they were specified) ---
    if course_tag and course_tag not in tag_names:
        issues.append(f"Missing: {course_tag}")
    if module_tag and module_tag not in tag_names:
        issues.append(f"Missing: {module_tag}")
    if unit_tags and not any(unit in tag_names for unit in unit_tags):
        issues.append(f"Missing: One of {', '.join(unit_tags)}")
    if company_tag and company_tag not in tag_names:
        issues.append(f"Missing: {company_tag}")

    # --- 4. Check for FOUND optional tags (if they were NOT specified) ---
    if not course_tag:
        if found_course := next((t for t in tag_names if t.startswith("COURSE_")), None):
            issues.append(f"Found Optional: {found_course}")
    if not module_tag:
        if found_module := next((t for t in tag_names if t.startswith("MODULE_")), None):
            issues.append(f"Found Optional: {found_module}")
    if not unit_tags:
        for tag in tag_names:
            if tag.startswith("UNIT_"):
                issues.append(f"Found Optional: {tag}")
    if not company_tag:
        if found_company := next((t for t in tag_names if t.startswith("COMPANY_")), None):
            issues.append(f"Found Optional: {found_company}")

    return qid, issues

# --- Initialize Session State ---
for key in ['formatted_course_tag', 'formatted_module_tag', 'formatted_unit_tag', 'extra_unit_tag', 'formatted_company_tag']:
    if key not in st.session_state: st.session_state[key] = ""
if 'debug_mode' not in st.session_state: st.session_state['debug_mode'] = False


# --- Streamlit UI ---
st.title("📦 Question Tag Validator App")
st.markdown("Upload question files to check for **missing**, **invalid**, or **unexpected** tags.")
st.markdown("---")

st.session_state.debug_mode = st.checkbox("🔍 Debug Mode (show detailed log for every question)", value=st.session_state.debug_mode)
st.markdown("---")

st.header("1. Enter Optional Tags for Validation")
st.markdown("*Enter tag names here to check for their presence. If left blank, the tool will instead report if it finds any unexpected tags of that type.*")

col1, col2, col3 = st.columns(3)
with col1:
    course_input = st.text_input("Course Name", key="course_raw", placeholder="e.g., Python")
    st.session_state.formatted_course_tag = format_tag_name(course_input, "COURSE_")
    if st.session_state.formatted_course_tag:
        st.success(f"✅ Will check for: **{st.session_state.formatted_course_tag}**")

with col2:
    module_input = st.text_input("Module Name", key="module_raw", placeholder="e.g., Looping")
    st.session_state.formatted_module_tag = format_tag_name(module_input, "MODULE_")
    if st.session_state.formatted_module_tag:
        st.success(f"✅ Will check for: **{st.session_state.formatted_module_tag}**")

with col3:
    unit_input = st.text_input("Unit Name", key="unit_raw", placeholder="e.g., Nested Conditions")
    st.session_state.formatted_unit_tag = format_tag_name(unit_input, "UNIT_")
    if st.session_state.formatted_unit_tag:
        st.success(f"✅ Formatted: **{st.session_state.formatted_unit_tag}**")

    extra_unit_input = st.text_input("Additional Unit Name", key="extra_unit_raw", placeholder="e.g., Loops")
    st.session_state.extra_unit_tag = format_tag_name(extra_unit_input, "UNIT_")
    if st.session_state.extra_unit_tag:
        st.success(f"✅ Additional Unit: **{st.session_state.extra_unit_tag}**")

company_input = st.text_input("Company Name", key="company_raw", placeholder="e.g., TCS")
st.session_state.formatted_company_tag = format_tag_name(company_input, "COMPANY_")
if st.session_state.formatted_company_tag:
    st.success(f"✅ Will check for: **{st.session_state.formatted_company_tag}**")

st.markdown("---")
st.header("2. Upload Question Files")
mcq_file = st.file_uploader("📄 Upload MCQ Excel file (.xlsx or .csv)", type=["xlsx", "csv"])
json_zip_file = st.file_uploader("📁 Upload JSON zip file (Coding/Code Analysis)", type=["zip"])
st.markdown("---")

st.header("3. Run Validation")
if st.button("🚀 Run Tag Check", type="primary"):
    if not (mcq_file or json_zip_file):
        st.warning("Please upload at least one file in Section 2.")
    else:
        with st.spinner("Processing files and validating tags..."):
            course_tag = st.session_state.formatted_course_tag
            module_tag = st.session_state.formatted_module_tag
            company_tag = st.session_state.formatted_company_tag
            unit_tags = [t for t in [st.session_state.formatted_unit_tag, st.session_state.extra_unit_tag] if t]

            active_tags = [t for t in [course_tag, module_tag, company_tag] + unit_tags if t]
            if active_tags:
                st.info(f"Validating for specified optional tags: **{', '.join(active_tags)}**")
            else:
                st.info("No optional tags specified. Will report any found `COURSE_`, `MODULE_`, `UNIT_`, or `COMPANY_` tags as 'Found Optional'.")

            all_questions_with_issues = []
            validation_details = []

            # <<< CHANGE 2: ADAPT MAIN LOOP TO HANDLE THE NEW `issues` LIST >>>
            def process_questions(questions, file_type):
                st.subheader(f"📊 Processing {file_type} File...")
                st.info(f"Found {len(questions)} questions. Validating...")
                for q in questions:
                    module_type = file_type
                    if file_type == "JSON": # Determine module type from JSON content
                        qtype = q.get("question_type", "")
                        if qtype == "CODE_ANALYSIS_MULTIPLE_CHOICE": module_type = "Code Analysis"
                        elif qtype == "CODING": module_type = "Web Coding" if q.get("question_format") == "WEB_CODING" else "Python Coding"
                        else: module_type = "Unknown JSON Type"
                    
                    qid, issues = validate_question_tags(q, module_type, unit_tags, course_tag, module_tag, company_tag)
                    has_issues = len(issues) > 0
                    
                    validation_details.append({
                        "question_id": qid, "module_type": module_type,
                        "current_tags": sorted(list(q.get("tag_names", []))),
                        "issues": issues, "has_issues": has_issues
                    })
                    if has_issues:
                        all_questions_with_issues.append({
                            "Question ID": qid, "Module Type": module_type,
                            "Issues Found": ", ".join(issues),
                            "Current Tags": ", ".join(sorted(list(q.get("tag_names", []))))
                        })

            if mcq_file:
                process_questions(extract_mcq_data(mcq_file), "MCQ")
            if json_zip_file:
                process_questions(extract_json_files(json_zip_file), "JSON")

        st.markdown("---")
        st.header("4. Validation Results")

        if st.session_state.debug_mode and validation_details:
            st.subheader("🔍 Detailed Validation Log")
            for detail in validation_details:
                status = '❌ Has Issues' if detail['has_issues'] else '✅ OK'
                with st.expander(f"Question: {detail['question_id']} ({status})"):
                    st.write(f"**Module Type:** {detail['module_type']}")
                    st.write(f"**Current Tags:** `{', '.join(detail['current_tags'])}`")
                    if detail['issues']:
                        st.error(f"**Issues:** {'; '.join(detail['issues'])}")

        # <<< CHANGE 3: UPDATE THE FINAL REPORTING SECTION >>>
        if all_questions_with_issues:
            result_df = pd.DataFrame(all_questions_with_issues)
            st.subheader("❌ Questions with Issues")
            st.dataframe(result_df, use_container_width=True)

            st.subheader("📈 Summary")
            total_q = len(validation_details)
            issues_q = len(all_questions_with_issues)
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Questions Processed", total_q)
            c2.metric("Questions with Issues", issues_q)
            if total_q > 0:
                c3.metric("Success Rate", f"{((total_q - issues_q) / total_q * 100):.1f}%")

            st.download_button("⬇️ Download Report as CSV", result_df.to_csv(index=False).encode('utf-8'), "tag_issues_report.csv", "text/csv")
        elif validation_details:
            st.success("✅ All questions are properly tagged! No issues found.")
        else:
            st.warning("No questions were extracted from the uploaded files.")


st.markdown("---")
with st.expander("📋 Tag Rules & File Formats"):
    st.markdown("""
    ### **Required Tags (MUST be present)**
    - `NIAT`, `IN_OFFLINE_EXAM`, `POOL_1`
    - One of: `DIFFICULTY_EASY`, `DIFFICULTY_MEDIUM`, `DIFFICULTY_HARD`
    - A `SOURCE_*` tag (e.g., `SOURCE_GPT`)
    - The Question ID (UUID) itself.
    - **Conditional:** `IS_PUBLIC` (for MCQ/Code Analysis) or `IS_PRIVATE` (for Coding).

    ### **Optional Tags (Validation Behavior)**
    - This tool checks for `COURSE_`, `MODULE_`, `UNIT_`, and `COMPANY_` tags.
    - **If you enter a tag name in Section 1:** The tool will report it as **`Missing`** if it's not found on a question.
    - **If you leave a field in Section 1 blank:** The tool will report it as **`Found Optional`** if it finds a corresponding tag on a question (e.g., finds a `COURSE_` tag when you didn't specify one).

    ### **Invalid Tags**
    - The tool will report tags as **`Invalid`** if they are wrong for the context, such as:
        - `IS_PUBLIC` on a Coding question.
        - A `TOPIC_` or `SUB_TOPIC_` tag that is not in the official list.
    """)