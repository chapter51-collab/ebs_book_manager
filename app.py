import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import re 
import uuid 
import io 
import os
import pickle
import base64
import hashlib
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- 1. 페이지 기본 설정 ---
st.set_page_config(
    page_title="EBS 교재개발 관리 프로그램",
    page_icon="📚",
    layout="wide"
)

# --- 2. 구글 시트 연동 설정 ---
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
SHEET_NAME = "EBS_Book_DB" 

def get_db_connection():
    try:
        if os.path.exists("service_account.json"):
            creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", SCOPE)
        elif "gcp_service_account" in st.secrets:
            creds_dict = dict(st.secrets["gcp_service_account"])
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
        else:
            return None
        
        client = gspread.authorize(creds)
        sheet = client.open(SHEET_NAME).sheet1
        return sheet
    except Exception as e:
        return None

def load_data_from_sheet():
    sheet = get_db_connection()
    if sheet:
        try:
            col_values = sheet.col_values(1)
            if col_values:
                full_b64_str = "".join(col_values)
                binary_data = base64.b64decode(full_b64_str)
                return pickle.loads(binary_data)
        except Exception as e:
            pass
    return []

def save_data_to_sheet(data):
    sheet = get_db_connection()
    if sheet:
        try:
            binary_data = pickle.dumps(data)
            b64_str = base64.b64encode(binary_data).decode('utf-8')
            chunk_size = 45000
            chunks = [b64_str[i:i+chunk_size] for i in range(0, len(b64_str), chunk_size)]
            sheet.clear()
            update_values = [[chunk] for chunk in chunks]
            sheet.update(range_name=f'A1:A{len(chunks)}', values=update_values)
            return True
        except Exception as e:
            st.error(f"저장 실패: {e}")
            return False
    return False

# --- 3. 데이터 초기화 ---
if 'projects' not in st.session_state:
    with st.spinner("☁️ 구글 시트에서 데이터를 불러오는 중..."):
        loaded_data = load_data_from_sheet()
        if loaded_data:
            st.session_state['projects'] = loaded_data
            st.toast("☁️ 클라우드에서 데이터를 성공적으로 불러왔습니다.")
        else:
            st.session_state['projects'] = []
            if os.path.exists("book_project_data.pkl"):
                 try:
                    with open("book_project_data.pkl", 'rb') as f:
                        st.session_state['projects'] = pickle.load(f)
                    st.toast("📂 로컬 백업 파일에서 데이터를 불러왔습니다.")
                 except: pass

# 데이터 정합성 검사 및 복구
for p in st.session_state['projects']:
    if 'created_at' not in p: p['created_at'] = datetime.now()
    if 'settlement_overrides' not in p: p['settlement_overrides'] = {} 
    
    if p.get('author_list') is None: p['author_list'] = []
    if p.get('reviewer_list') is None: p['reviewer_list'] = []
    if p.get('partner_list') is None: p['partner_list'] = []
    
    if 'settlement_list' not in p or p['settlement_list'] is None:
        p['settlement_list'] = []

if 'current_project_id' not in st.session_state:
    st.session_state['current_project_id'] = None 
if 'selected_overview_id' not in st.session_state:
    st.session_state['selected_overview_id'] = None
if 'view_all_mode' not in st.session_state:
    st.session_state['view_all_mode'] = False

def normalize_string(s):
    return str(s).replace(" ", "").strip()

def clean_korean_date(date_str):
    if pd.isna(date_str): return None
    s = str(date_str)
    s = re.sub(r'\s*\(.*?\)', '', s)
    return s.strip()

DEFAULT_CHECKLIST = [
    {"구분": "결과보고서", "내용": "결과보고서 작성", "완료": False},
    {"구분": "결과보고서", "내용": "집필자 성과 평가 작성", "완료": False},
    {"구분": "결과보고서", "내용": "검토자 역량 평가", "완료": False},
    {"구분": "약정서(집필자)", "내용": "집필약정서", "완료": False},
    {"구분": "약정서(집필자)", "내용": "보안서약서", "완료": False},
    {"구분": "약정서(집필자)", "내용": "수의계약체결제한여부확인서", "완료": False},
    {"구분": "약정서(집필자)", "내용": "청렴계약이행서약서", "완료": False},
    {"구분": "약정서(검토자)", "내용": "검토약정서", "완료": False},
    {"구분": "약정서(검토자)", "내용": "보안서약서", "완료": False},
    {"구분": "약정서(검토자)", "내용": "수의계약체결제한여부확인서", "완료": False},
    {"구분": "약정서(검토자)", "내용": "청렴계약이행서약서", "완료": False},
    {"구분": "회의록", "내용": "제작관련업체 사전협의회(인쇄협의체) 회의록", "완료": False},
    {"구분": "회의록", "내용": "편집대행서 최종 점검 체크리스트", "완료": False},
]

for p in st.session_state['projects']:
    new_auth_std = pd.DataFrame([
        {"구분": "쪽당", "원고료": 35000, "검토료": 14000},
        {"구분": "문항당", "원고료": 3000, "검토료": 1500}
    ])

    keys_defaults = {
        "author_list": [], "reviewer_list": [], "partner_list": [], "issues": [],
        "dev_data": pd.DataFrame(columns=["단원명", "집필자", "집필완료", "검토완료", "피드백완료", "디자인완료", "비고"]),
        "planning_data": pd.DataFrame(), "schedule_data": pd.DataFrame(),
        "book_specs": {"format": "", "colors_main": ["1도"], "colors_sol": "1도", "is_ebook": False, "is_answer_view": False, "is_answer_pdf": False},
        "report_checklist": pd.DataFrame(DEFAULT_CHECKLIST), 
        "author_standards": new_auth_std, 
        "review_standards": pd.DataFrame([
            {"구분": "1차외부검토", "단가(쪽)": 8000, "단가(문항)": 1000},
            {"구분": "2차외부검토", "단가(쪽)": 8000, "단가(문항)": 1000},
            {"구분": "3차외부검토", "단가(쪽)": 8000, "단가(문항)": 1000},
            {"구분": "편집검토", "단가(쪽)": 6000, "단가(문항)": 500}
        ]),
        "penalties": {},
        "target_date_val": datetime.today(),
        "created_at": datetime.now(),
        "settlement_list": []
    }
    
    for key, default_val in keys_defaults.items():
        if key not in p: 
            p[key] = default_val
        elif key == "author_standards":
            current_std = p['author_standards']
            if '원고료_단가(쪽)' in current_std.columns: 
                old_row = current_std.iloc[0]
                p['author_standards'] = pd.DataFrame([
                    {"구분": "쪽당", "원고료": old_row.get('원고료_단가(쪽)', 35000), "검토료": old_row.get('검토료_단가(쪽)', 14000)},
                    {"구분": "문항당", "원고료": old_row.get('원고료_단가(문항)', 3000), "검토료": old_row.get('검토료_단가(문항)', 1500)}
                ])
            elif '원고료_단가' in current_std.columns:
                old_row = current_std.iloc[0]
                p['author_standards'] = pd.DataFrame([
                    {"구분": "쪽당", "원고료": old_row.get('원고료_단가', 35000), "검토료": old_row.get('검토료_단가', 14000)},
                    {"구분": "문항당", "원고료": 3000, "검토료": 1500}
                ])

    if 'report_checklist' in p:
        if len(p['report_checklist']) < 3:
            p['report_checklist'] = pd.DataFrame(DEFAULT_CHECKLIST)
            
    rev_std = p['review_standards']
    if '단가(문항)' not in rev_std.columns:
        rev_std['단가(문항)'] = 1000
        if '단가' in rev_std.columns: rev_std.rename(columns={'단가': '단가(쪽)'}, inplace=True)
        p['review_standards'] = rev_std

    if 'dev_data' in p:
        if p['dev_data'].empty:
             p['dev_data'] = pd.DataFrame(columns=["단원명", "집필자", "집필완료", "검토완료", "피드백완료", "디자인완료", "비고"])
        else:
            new_cols = {c: normalize_string(c) for c in p['dev_data'].columns}
            p['dev_data'] = p['dev_data'].rename(columns=new_cols)
            rename_map = {"1차검토자": "1차외부검토", "2차검토자": "2차외부검토", "3차검토자": "3차외부검토"}
            p['dev_data'] = p['dev_data'].rename(columns=rename_map)
            bool_cols = ["집필완료", "검토완료", "피드백완료", "디자인완료"]
            for col in bool_cols:
                if col not in p['dev_data'].columns: p['dev_data'][col] = False
                else: p['dev_data'][col] = p['dev_data'][col].astype(bool)

    active_roles = set(["1차외부검토", "2차외부검토", "3차외부검토", "편집검토"]) 
    if 'reviewer_list' in p:
        for r in p['reviewer_list']:
            role = r.get('검토차수')
            if role: active_roles.add(normalize_string(role))

    rev_std = p['review_standards']
    rev_std['구분_clean'] = rev_std['구분'].apply(normalize_string)
    existing_std = set(rev_std['구분_clean'].tolist())
    new_std_rows = []
    for role in active_roles:
        if role not in existing_std: new_std_rows.append({"구분": role, "단가(쪽)": 0, "단가(문항)": 0})
    if new_std_rows:
        p['review_standards'] = pd.concat([rev_std.drop(columns=['구분_clean']), pd.DataFrame(new_std_rows)], ignore_index=True)
    elif '구분_clean' in rev_std.columns:
        p['review_standards'] = rev_std.drop(columns=['구분_clean'])

    if 'dev_data' in p:
        current_cols = p['dev_data'].columns
        for role in active_roles:
            if role not in current_cols: p['dev_data'][role] = "-"

# --- 4. 유틸리티 함수 ---
def get_day_name(date_obj):
    if pd.isnull(date_obj): return ""
    try: return ["(월)", "(화)", "(수)", "(목)", "(금)", "(토)", "(일)"][date_obj.weekday()]
    except: return ""

def validate_email(email): return "@" in str(email)

def get_schedule_date(project, keyword="플루토"):
    df = project.get('schedule_data', pd.DataFrame())
    if df.empty: return None
    mask = df['구분'].astype(str).str.contains(keyword, na=False)
    if mask.any():
        try:
            date_val = df.loc[mask, '종료일'].values[-1]
            return pd.to_datetime(date_val)
        except: return None
    return None

def get_notifications():
    notifications = []
    today = datetime.now().date()
    alert_window = 3 
    for p in st.session_state['projects']:
        sch = p.get('schedule_data')
        if sch is not None and not sch.empty:
            for _, row in sch.iterrows():
                try:
                    end_date = pd.to_datetime(row['종료일']).date()
                    if pd.notnull(end_date):
                        days_left = (end_date - today).days
                        if 0 <= days_left <= alert_window:
                            notifications.append({
                                "project": f"[{p['series']}] {p['title']}",
                                "task": row['구분'],
                                "date": end_date,
                                "d_day": days_left
                            })
                except: continue
    return notifications

def create_ics_file(df, project_title):
    ics_content = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//EBS 교재개발 관리 프로그램//Streamlit App//KO",
        "X-WR-CALNAME:EBS " + project_title + " 개발 일정"
    ]
    for _, row in df.iterrows():
        if pd.isnull(row['시작일']) or pd.isnull(row['종료일']): continue
        try:
            start_date = row['시작일'].strftime('%Y%m%d')
            end_date = (pd.to_datetime(row['종료일']).date() + timedelta(days=1)).strftime('%Y%m%d')
            ics_content.extend([
                "BEGIN:VEVENT",
                f"UID:{uuid.uuid4()}@ebs.co.kr",
                f"DTSTAMP:{datetime.now().strftime('%Y%m%dT%H%M%SZ')}",
                f"DTSTART;VALUE=DATE:{start_date}",
                f"DTEND;VALUE=DATE:{end_date}",
                f"SUMMARY:{row['구분']}",
                f"DESCRIPTION:{row['비고']}",
                "END:VEVENT"
            ])
        except: continue
    ics_content.append("END:VCALENDAR")
    return "\n".join(ics_content).encode('utf-8')

# --- 5. 데이터 안전장치 함수 ---
def ensure_data_types(df):
    df = df.copy()
    df = df.reset_index(drop=True)
    df["시작일"] = pd.to_datetime(df["시작일"], errors='coerce').dt.date
    df["종료일"] = pd.to_datetime(df["종료일"], errors='coerce').dt.date
    df["소요 일수"] = pd.to_numeric(df["소요 일수"], errors='coerce').fillna(0).astype(int)
    df["선택"] = df["선택"].astype(bool)
    df["독립 일정"] = df["독립 일정"].astype(bool)
    return df

# --- 6. 핵심 로직 (일정) ---
def recalculate_dates(df, target_date_obj):
    df["시작일"] = pd.to_datetime(df["시작일"])
    df["종료일"] = pd.to_datetime(df["종료일"])
    
    anchor_mask = df["구분"].str.contains("최종 플루토 OK", na=False)
    if not anchor_mask.any():
        if len(df) > 0: anchor_idx = df.index[-1]
        else: return ensure_data_types(df)
    else: anchor_idx = df[anchor_mask].index[0]

    current_end = pd.to_datetime(target_date_obj)
    df.at[anchor_idx, "종료일"] = current_end
    duration = int(df.at[anchor_idx, "소요 일수"]) 
    df.at[anchor_idx, "시작일"] = current_end - timedelta(days=max(0, duration - 1))

    # Backward
    chain_link_date = df.at[anchor_idx, "시작일"]
    for i in range(anchor_idx - 1, -1, -1):
        if df.at[i, "독립 일정"]: continue 
        current_end = chain_link_date - timedelta(days=1)
        df.at[i, "종료일"] = current_end
        duration = int(df.at[i, "소요 일수"])
        current_start = current_end - timedelta(days=max(0, duration - 1))
        df.at[i, "시작일"] = current_start
        chain_link_date = current_start

    # Forward
    chain_link_date = df.at[anchor_idx, "종료일"]
    for i in range(anchor_idx + 1, len(df)):
        if df.at[i, "독립 일정"]: continue
        current_start = chain_link_date + timedelta(days=1)
        df.at[i, "시작일"] = current_start
        duration = int(df.at[i, "소요 일수"])
        current_end = current_start + timedelta(days=max(0, duration - 1))
        df.at[i, "종료일"] = current_end
        chain_link_date = current_end
    return ensure_data_types(df)

# 중요 키워드
IMPORTANT_KEYWORDS = ["발주 회의", "집필 (본문 개발)", "1차 외부/교차 검토", "2차 외부/교차 검토", "3차 외부/교차 검토", "가쇄본 제작", "집필자 최종 검토", "내용 OK", "최종 플루토 OK", "플루토"]

def create_initial_schedule(target_date_obj):
    schedule_list = []
    base_date = pd.to_datetime(target_date_obj)
    current_end = base_date
    
    def add_row_backward(name, days, independent=False, note=""):
        nonlocal current_end
        display_name = name
        if any(keyword in name for keyword in IMPORTANT_KEYWORDS): display_name = f"🔴 {name}"
        start = current_end - timedelta(days=days - 1)
        schedule_list.append({
            "선택": False, "독립 일정": independent, "구분": display_name, "소요 일수": days, 
            "시작일": start.date(), "종료일": current_end.date(), "비고": note
        })
        if not independent: current_end = start - timedelta(days=1)

    add_row_backward("최종 플루토 OK", 2, note="★ 확정일 (기준)") 
    add_row_backward("내용 OK", 3)
    print_mtg_date = current_end - timedelta(days=14)
    schedule_list.append({"선택": False, "독립 일정": True, "구분": "인쇄협의체 회의", "소요 일수": 1, "시작일": print_mtg_date.date(), "종료일": print_mtg_date.date(), "비고": "독립 일정"})
    add_row_backward("최종 검토 반영", 7)
    add_row_backward("집필자 최종 검토", 1)
    add_row_backward("편집 검토", 7)
    add_row_backward("가쇄본 제작", 3) 
    for i in range(3, 0, -1):
        add_row_backward(f"{i}차 조판 수정", 7)
        add_row_backward(f"{i}차 집필자 반영", 7)
        add_row_backward(f"{i}차 외부/교차 검토", 7) 
    add_row_backward("1차 조판 및 편집", 40)
    add_row_backward("  └ 최종 집필물 수령", 0, independent=True)
    add_row_backward("  ├ 1차 집필물 수령", 0, independent=True)
    add_row_backward("집필 (본문 개발)", 30) 
    add_row_backward("발주 회의 및 계약", 1)
    pre_steps = ["샘플 원고 작성", "발주회의 자료 제작", "집필자 섭외", "배열표 작성", "일정 확정", "기획안 확인"]
    for name in pre_steps: add_row_backward(name, 1, independent=False, note="직접 입력")
    schedule_list.reverse()
    
    pdf_start = base_date + timedelta(days=1)
    pdf_end = pdf_start + timedelta(days=3 - 1)
    schedule_list.append({"선택": False, "독립 일정": False, "구분": "최종 PDF 수령", "소요 일수": 3, "시작일": pdf_start.date(), "종료일": pdf_end.date(), "비고": "OK 이후 진행"})
    report_date = base_date + timedelta(days=30)
    schedule_list.append({"선택": False, "독립 일정": False, "구분": "📝 개발완료보고서 작성", "소요 일수": 1, "시작일": report_date.date(), "종료일": report_date.date(), "비고": "기준일 + 1개월 내"})
    settlement_date = base_date + timedelta(days=90)
    schedule_list.append({"선택": False, "독립 일정": False, "구분": "💰 개발비 정산", "소요 일수": 0, "시작일": settlement_date.date(), "종료일": settlement_date.date(), "비고": "기준일 + 3개월 내"})
    return pd.DataFrame(schedule_list).reset_index(drop=True)

# --- 7. 교재(프로젝트) 관리 함수 ---
def get_project_by_id(pid):
    for p in st.session_state['projects']:
        if p['id'] == pid: return p
    return None

def update_current_project_data(key, value):
    pid = st.session_state['current_project_id']
    for p in st.session_state['projects']:
        if p['id'] == pid:
            p[key] = value
            break

def create_new_project():
    year = st.session_state.new_proj_year
    level = st.session_state.new_proj_level
    subject = st.session_state.new_proj_subject 
    series = st.session_state.new_proj_series
    title = st.session_state.new_proj_title

    if not series or not title:
        st.error("시리즈명과 교재명은 필수 입력입니다.")
        return

    new_p = {
        "id": str(uuid.uuid4()), "year": year, "level": level, "subject": subject, "series": series, "title": title,
        "schedule_data": pd.DataFrame(), "author_list": [], "reviewer_list": [], "partner_list": [], 
        "dev_data": pd.DataFrame(columns=["단원명", "집필자", "집필상태", "원고파일", "검토자", "검토상태", "피드백", "디자인상태", "비고"]), 
        "issues": [], "planning_data": pd.DataFrame(), 
        "book_specs": {"format": "", "colors_main": ["1도"], "colors_sol": "1도", "is_ebook": False, "is_answer_view": False, "is_answer_pdf": False},
        "report_checklist": pd.DataFrame(DEFAULT_CHECKLIST),
        "author_standards": pd.DataFrame([
            {"구분": "쪽당", "원고료": 35000, "검토료": 14000},
            {"구분": "문항당", "원고료": 3000, "검토료": 1500}
        ]),
        "review_standards": pd.DataFrame([
            {"구분": "1차외부검토", "단가(쪽)": 8000, "단가(문항)": 1000},
            {"구분": "2차외부검토", "단가(쪽)": 8000, "단가(문항)": 1000},
            {"구분": "3차외부검토", "단가(쪽)": 8000, "단가(문항)": 1000},
            {"구분": "편집검토", "단가(쪽)": 6000, "단가(문항)": 500}
        ]),
        "penalties": {},
        "target_date_val": datetime.today(),
        "created_at": datetime.now(),
        "settlement_list": []
    }
    
    default_target = datetime.today()
    new_p['schedule_data'] = create_initial_schedule(default_target)
    new_p['target_date_val'] = default_target

    st.session_state['projects'].append(new_p)
    st.session_state['current_project_id'] = new_p['id'] 
    st.success(f"[{series}] {title} 교재가 생성되었습니다!")
    st.rerun()

# --- 8. 사이드바 ---
st.sidebar.title("📚 EBS 교재개발 관리")

# [MD5 Hash Change Detection]
def get_data_hash(data):
    return hashlib.md5(pickle.dumps(data)).hexdigest()

if 'last_saved_hash' not in st.session_state:
    st.session_state['last_saved_hash'] = get_data_hash(st.session_state['projects'])

current_hash = get_data_hash(st.session_state['projects'])
has_changes = current_hash != st.session_state['last_saved_hash']

if has_changes:
    st.sidebar.markdown(
        """
        <div style="
            animation: pulse 2s infinite; 
            background-color: #ff4b4b; 
            color: white; 
            padding: 10px; 
            border-radius: 5px; 
            text-align: center; 
            margin-bottom: 10px;
            font-weight: bold;">
            ⚠️ 저장되지 않은 변경사항이 있습니다!
        </div>
        <style>
            @keyframes pulse {
                0% { opacity: 1; }
                50% { opacity: 0.7; }
                100% { opacity: 1; }
            }
        </style>
        """, 
        unsafe_allow_html=True
    )
    save_btn_label = "💾 변경 사항 저장 (Click!)"
    save_btn_type = "primary"
else:
    save_btn_label = "✅ 최신 상태입니다"
    save_btn_type = "secondary"

if st.sidebar.button(save_btn_label, type=save_btn_type):
    with st.spinner("구글 시트에 저장 중..."):
        if save_data_to_sheet(st.session_state['projects']):
            st.session_state['last_saved_hash'] = get_data_hash(st.session_state['projects'])
            st.sidebar.success("✅ 안전하게 저장되었습니다!")
            st.rerun()
        else:
            st.sidebar.error("저장 실패. service_account.json 파일이나 인터넷 연결을 확인하세요.")

# [Emergency Reload]
if st.sidebar.button("🔄 서버 데이터 다시 불러오기 (수정 취소)"):
    with st.spinner("서버에서 데이터를 다시 가져오는 중..."):
        reloaded = load_data_from_sheet()
        if reloaded:
            st.session_state['projects'] = reloaded
            st.session_state['last_saved_hash'] = get_data_hash(reloaded)
            st.sidebar.success("데이터를 복구했습니다.")
            st.rerun()

current_p = get_project_by_id(st.session_state['current_project_id'])

st.sidebar.markdown("---")
st.sidebar.header("🚀 메뉴 이동")
menu = st.sidebar.radio(
    "메뉴 이동",
    ["교재 등록 및 관리(HOME)", "1. 교재 기획", "2. 개발 일정", "3. 참여자", "4. 개발 프로세스", "5. 결과보고서 및 정산"],
    key="main_menu",
    label_visibility="collapsed"
)

if current_p:
    st.sidebar.markdown("---")
    st.sidebar.markdown("**📂 현재 작업 중인 교재**")
    st.sidebar.info(f"**[{current_p['year']}/{current_p['level']}]**\n\n{current_p['series']} - {current_p['title']}")
else:
    st.sidebar.markdown("---")
    st.sidebar.warning("선택된 교재가 없습니다.\nHOME에서 교재를 선택해주세요.")

# --- [Popup Function] 신규 교재 등록 ---
@st.dialog("✨ 새로운 교재 등록")
def entry_dialog():
    col_new1, col_new2, col_new3 = st.columns(3)
    with col_new1: new_year = st.selectbox("발행 연도", [str(y) for y in range(2025, 2031)], key="modal_new_proj_year")
    with col_new2: new_level = st.selectbox("학교급", ["초등", "중학", "고교", "기타"], key="modal_new_proj_level")
    with col_new3: new_subject = st.selectbox("과목", ["국어", "영어", "수학", "사회", "과학", "종합", "기타"], key="modal_new_proj_subject")
    
    col_new4, col_new5 = st.columns([1, 2])
    with col_new4: new_series = st.text_input("시리즈명", key="modal_new_proj_series")
    with col_new5: new_title = st.text_input("교재명", key="modal_new_proj_title")
    
    if st.button("🚀 등록하기", type="primary"):
        if not new_series or not new_title:
            st.error("시리즈명과 교재명은 필수입니다.")
        else:
            st.session_state.new_proj_year = new_year
            st.session_state.new_proj_level = new_level
            st.session_state.new_proj_subject = new_subject
            st.session_state.new_proj_series = new_series
            st.session_state.new_proj_title = new_title
            create_new_project()
            st.rerun()

# --- 10. 메인 화면 ---

if menu == "교재 등록 및 관리(HOME)":
    st.title("📊 교재 등록 및 관리 Dashboard")
    
    # 1. 상단 요약 배너 (Metrics)
    total_cnt = len(st.session_state['projects'])
    impending_cnt = 0
    completed_cnt = 0
    today = datetime.now().date()
    
    for p in st.session_state['projects']:
        # 마감 임박 (3일 내)
        sch = p.get('schedule_data')
        if sch is not None and not sch.empty:
            for _, row in sch.iterrows():
                try:
                    ed = pd.to_datetime(row['종료일']).date()
                    if pd.notnull(ed):
                        days = (ed - today).days
                        if 0 <= days <= 3:
                            impending_cnt += 1
                            break 
                except: pass
        # 완료 (플루토 OK)
        target_date = get_schedule_date(p)
        if target_date and target_date.date() < today:
            completed_cnt += 1

    col_m1, col_m2, col_m3 = st.columns(3)
    col_m1.metric("📚 전체 교재", f"{total_cnt}권")
    col_m2.metric("🔴 마감 임박 (3일 내)", f"{impending_cnt}건")
    col_m3.metric("🟢 완료 (플루토 OK)", f"{completed_cnt}권")

    st.markdown("---")

    # 2. 중단 (좌: 알림 / 우: 컨트롤)
    col_home_L, col_home_R = st.columns([1, 1.3])

    with col_home_L:
        st.subheader("🔔 마감 임박 알림")
        with st.container(height=300): # 고정 높이 스크롤
            alerts = get_notifications() # D-3 로직 반영
            if not alerts:
                st.info("🎉 3일 이내 마감되는 일정이 없습니다.")
            else:
                for a in alerts:
                    if a['d_day'] < 0:
                        st.error(f"**{a['project']}**\n- {a['task']} (마감일: {a['date']}, D+{abs(a['d_day'])})")
                    elif a['d_day'] == 0:
                        st.error(f"**{a['project']}**\n- {a['task']} (오늘 마감!)")
                    else:
                        st.warning(f"**{a['project']}**\n- {a['task']} (마감일: {a['date']}, D-{a['d_day']})")

    with col_home_R:
        st.subheader("🛠️ 관리 및 검색")
        
        if st.button("✨ 새 교재 등록 (팝업 열기)", use_container_width=True):
            entry_dialog()

        st.markdown("##### 🔍 검색 필터")
        all_years = sorted(list(set([p['year'] for p in st.session_state['projects']])))
        all_levels = ["초등", "중학", "고교", "기타"]
        all_subjects = sorted(list(set([p.get('subject', '-') for p in st.session_state['projects']])))

        c_f1, c_f2, c_f3 = st.columns(3)
        with c_f1: s_year = st.selectbox("발행 연도", ["전체"] + all_years, key='filter_year_new')
        with c_f2: s_level = st.selectbox("학교급", ["전체"] + all_levels, key='filter_level_new')
        with c_f3: s_subject = st.selectbox("과목", ["전체"] + all_subjects, key='filter_subject_new')

        filtered_list = []
        for p in st.session_state['projects']:
            if s_year != "전체" and p['year'] != s_year: continue
            if s_level != "전체" and p['level'] != s_level: continue
            if s_subject != "전체" and p.get('subject','-') != s_subject: continue
            filtered_list.append(p)
        
        if st.button("🔄 교재 목록 펼치기/접기", use_container_width=True):
            st.session_state['view_all_mode'] = not st.session_state['view_all_mode']

    st.markdown("---")

    # 3. 하단 목록 테이블 (Single Selection Logic Applied)
    st.subheader("📋 교재 목록")
    
    is_filtered = (s_year != "전체" or s_level != "전체" or s_subject != "전체")
    show_table = is_filtered or st.session_state['view_all_mode']

    cols = ["선택", "삭제", "발행 연도", "학교급", "과목", "시리즈", "교재명", "최종 플루토 OK", "ID"]
    
    if show_table:
        table_data = []
        for p in filtered_list: 
            is_sel = (p['id'] == st.session_state['selected_overview_id'])
            t_date = get_schedule_date(p)
            t_str = t_date.strftime("%Y-%m-%d") if t_date else "-"
            table_data.append({
                "선택": is_sel, "삭제": False,
                "발행 연도": p['year'], "학교급": p['level'], "과목": p.get('subject','-'),
                "시리즈": p['series'], "교재명": p['title'], "최종 플루토 OK": t_str, "ID": p['id']
            })
        final_df = pd.DataFrame(table_data)
        if final_df.empty: final_df = pd.DataFrame(columns=cols)
    else:
        final_df = pd.DataFrame(columns=cols)

    edited_df = st.data_editor(
        final_df, hide_index=True, key="main_dash_editor",
        column_order=["선택", "발행 연도", "학교급", "과목", "시리즈", "교재명", "최종 플루토 OK", "삭제"],
        column_config={
            "선택": st.column_config.CheckboxColumn("선택", width="small"),
            "삭제": st.column_config.CheckboxColumn("삭제", width="small"),
        }
    )

    if not edited_df.empty:
        # 삭제 처리
        to_delete = edited_df[edited_df['삭제'] == True]
        if not to_delete.empty:
            if st.button("🗑️ 선택한 교재 영구 삭제", type="primary"):
                del_ids = to_delete['ID'].tolist()
                st.session_state['projects'] = [p for p in st.session_state['projects'] if p['id'] not in del_ids]
                if st.session_state['current_project_id'] in del_ids:
                    st.session_state['current_project_id'] = None
                st.rerun()
        
        # [NEW] 단일 선택 로직 (Radio Button Behavior)
        current_checked = edited_df[edited_df['선택'] == True]
        current_checked_ids = current_checked['ID'].tolist()
        prev_id = st.session_state['selected_overview_id']

        if len(current_checked_ids) > 1:
            # 여러 개가 체크된 경우: 기존 것 말고 '새로 체크된 것'을 찾음
            for pid in current_checked_ids:
                if pid != prev_id:
                    st.session_state['selected_overview_id'] = pid
                    st.session_state['current_project_id'] = pid
                    st.rerun()
                    break
        elif len(current_checked_ids) == 1:
            # 하나만 체크된 경우: 그게 기존과 다르다면 업데이트
            if current_checked_ids[0] != prev_id:
                st.session_state['selected_overview_id'] = current_checked_ids[0]
                st.session_state['current_project_id'] = current_checked_ids[0]
                st.rerun()
        elif len(current_checked_ids) == 0 and prev_id is not None:
            # 체크 해제된 경우
            st.session_state['selected_overview_id'] = None
            st.session_state['current_project_id'] = None
            st.rerun()

    # 4. 상세 개요
    if st.session_state['selected_overview_id']:
        sel_p = get_project_by_id(st.session_state['selected_overview_id'])
        if sel_p:
            st.info(f"📌 선택됨: **[{sel_p['series']}] {sel_p['title']}**")
            c_ov1, c_ov2 = st.columns(2)
            with c_ov1:
                st.caption("👥 참여자 요약")
                auths = [a['이름'] for a in sel_p['author_list']]
                st.write(f"집필: {', '.join(auths) if auths else '-'}")
                revs = [r['이름'] for r in sel_p['reviewer_list']]
                st.write(f"검토: {', '.join(revs) if revs else '-'}")
            with c_ov2:
                st.caption("📅 주요 일정")
                sch = ensure_data_types(sel_p['schedule_data'])
                if not sch.empty:
                    major = sch[sch['구분'].str.contains("🔴", na=False)]
                    if not major.empty:
                        for _, r in major.iterrows():
                            d = r['시작일'] if pd.notnull(r['시작일']) else r['종료일']
                            st.write(f"{d} : {r['구분'].replace('🔴 ','')}")
                    else: st.write("주요 일정 없음")
                else: st.write("일정 없음")

elif not current_p:
    st.title(f"{menu}")
    st.warning("⚠️ 교재가 선택되지 않았습니다.")

else:
    st.markdown(f"### 📂 [{current_p['year']}/{current_p['level']}] {current_p.get('subject','')} - {current_p['series']} {current_p['title']}")
    st.markdown("---")

    # ==========================================
    # [1. 교재 기획] 
    # ==========================================
    if menu == "1. 교재 기획":
        st.title("📝 교재 기획 (배열표 및 사양)")
        tab_plan1, tab_plan2 = st.tabs(["📊 배열표 작성", "📕 교재 기획 및 사양"])
        
        with tab_plan1:
            st.info("교재의 목차와 담당 집필자, 페이지 수, 문항 수 등을 관리합니다.")
            
            # --- DOWNLOAD BUTTON ---
            col_down, col_up = st.columns([1, 2])
            with col_down:
                 sample_data = {
                     "분권": ["Book1", "Book1", "Book1", "Book1", "Book1"],
                     "구분": ["속표지", "구성과 특징", "대단원도비라", "", ""],
                     "대단원": ["", "", "", "1. 화학의 언어", "1. 화학의 언어"],
                     "중단원": ["", "", "", "1. 생활 속 화학", "2. 화학 반응식"],
                     "쪽수": [1, 2, 12, 28, 19],
                     "문항수": [0, 0, 0, 15, 20], 
                     "집필자": ["", "", "", "노동규", "노동규"],
                     "비고": ["", "", "", "", ""]
                 }
                 df_sample = pd.DataFrame(sample_data)
                 csv_sample = df_sample.to_csv(index=False).encode('utf-8-sig')
                 
                 st.download_button(
                     label="⬇️ 표준 양식 다운로드",
                     data=csv_sample,
                     file_name="배열표_표준양식.csv",
                     mime="text/csv"
                 )
                 st.caption("⚠️ **주의:** 대단원명은 빈 셀 없이 채워주세요! 부속(속표지 등)에는 집필자를 비워주세요.")
            
            with col_up:
                if st.button("🔄 데이터 연동 (Sync)", type="primary"):
                    plan_df = current_p.get('planning_data', pd.DataFrame())
                    if not plan_df.empty:
                        # 1. Author list sync
                        if '집필자' in plan_df.columns:
                            existing = [a['이름'] for a in current_p.get('author_list', [])]
                            for auth in plan_df['집필자'].unique():
                                if pd.notnull(auth) and str(auth).strip() not in ['-', ''] and auth not in existing:
                                    current_p['author_list'].append({"이름": auth, "역할": "공동집필"})
                        
                        # 2. Dev Data Rebuild
                        if '대단원' in plan_df.columns:
                            current_dev_df = current_p.get('dev_data', pd.DataFrame())
                            existing_map = {}
                            if not current_dev_df.empty and '단원명' in current_dev_df.columns:
                                for _, row in current_dev_df.iterrows():
                                    existing_map[str(row['단원명'])] = row.to_dict()

                            new_rows = []
                            for _, row in plan_df.iterrows():
                                unit_name = f"[{row.get('분권','')}] {row.get('대단원','')} > {row.get('중단원','')}"
                                if unit_name in existing_map:
                                    new_rows.append(existing_map[unit_name])
                                else:
                                    new_base_row = {"단원명": unit_name, "집필자": row.get('집필자', '')}
                                    for col in current_dev_df.columns:
                                        if col not in new_base_row:
                                            new_base_row[col] = current_dev_df[col].iloc[0] if not current_dev_df.empty and isinstance(current_dev_df[col].iloc[0], bool) else ""
                                    new_rows.append(new_base_row)

                            new_dev_df = pd.DataFrame(new_rows)
                            if new_dev_df.empty:
                                new_dev_df = pd.DataFrame(columns=["단원명", "집필자", "집필완료", "검토완료", "피드백완료", "디자인완료", "비고"])
                            else:
                                for col in current_dev_df.columns:
                                    if col not in new_dev_df.columns:
                                        new_dev_df[col] = ""

                            current_p['dev_data'] = new_dev_df
                            st.toast("✅ 연동 및 동기화 완료 (삭제된 단원 정리됨)")
            
            uploaded_file = st.file_uploader("배열표 엑셀/CSV 파일 업로드", type=["xlsx", "xls", "csv"])
            if uploaded_file:
                try:
                    if uploaded_file.name.endswith('.csv'): df_upload = pd.read_csv(uploaded_file)
                    else: df_upload = pd.read_excel(uploaded_file)
                    
                    if '분권' in df_upload.columns: df_upload['분권'] = df_upload['분권'].fillna(method='ffill')
                    if '대단원' in df_upload.columns: df_upload['대단원'] = df_upload['대단원'].fillna(method='ffill')
                    if '구분' in df_upload.columns: df_upload['구분'] = df_upload['구분'].fillna("") 
                    if '문항수' not in df_upload.columns: df_upload['문항수'] = 0 

                    update_current_project_data('planning_data', df_upload)
                    st.success("파일 업로드 완료!")
                except Exception as e: st.error(f"파일 읽기 실패: {e}")

            plan_df = current_p.get('planning_data', pd.DataFrame())
            if not plan_df.empty:
                if '문항수' not in plan_df.columns: plan_df['문항수'] = 0

                edited_plan = st.data_editor(plan_df, num_rows="dynamic", key="planning_editor")
                if not edited_plan.equals(plan_df):
                    update_current_project_data('planning_data', edited_plan)
                
                # Graphs
                if '집필자' in plan_df.columns:
                    try:
                        col_g1, col_g2 = st.columns(2)
                        with col_g1:
                             if '쪽수' in plan_df.columns:
                                plan_df['쪽수_num'] = pd.to_numeric(plan_df['쪽수'], errors='coerce').fillna(0)
                                chart_data_page = plan_df.groupby('집필자')['쪽수_num'].sum().reset_index()
                                st.markdown("##### 📄 집필자별 쪽수")
                                st.bar_chart(chart_data_page.set_index('집필자'))
                        
                        with col_g2:
                             if '문항수' in plan_df.columns:
                                plan_df['문항수_num'] = pd.to_numeric(plan_df['문항수'], errors='coerce').fillna(0)
                                chart_data_item = plan_df.groupby('집필자')['문항수_num'].sum().reset_index()
                                st.markdown("##### ❓ 집필자별 문항수")
                                st.bar_chart(chart_data_item.set_index('집필자'), color="#FF6C6C") 

                    except Exception as e: pass
            else:
                if st.button("빈 배열표 생성"):
                    current_p['planning_data'] = pd.DataFrame(columns=["분권", "구분", "대단원", "중단원", "쪽수", "문항수", "집필자"])
                    st.rerun()

        with tab_plan2:
            st.subheader("교재 사양")
            if 'book_specs' not in current_p: current_p['book_specs'] = {}
            specs = current_p['book_specs']

            with st.container(border=True):
                col_spec1, col_spec2 = st.columns(2)
                with col_spec1:
                    new_format = st.text_input("판형 (Format)", value=specs.get("format", ""))
                    if new_format != specs.get("format"):
                        specs["format"] = new_format
                        update_current_project_data('book_specs', specs)

                st.markdown("#### 도수 (Colors)")
                if "colors_main" not in specs: specs["colors_main"] = ["1도"]
                
                for i, color in enumerate(specs["colors_main"]):
                    col_c1, col_c2 = st.columns([3, 1])
                    with col_c1:
                        new_color = st.radio(f"본문 {i+1}", ["1도", "2도", "4도"], key=f"color_main_{i}", horizontal=True, index=["1도", "2도", "4도"].index(color) if color in ["1도", "2도", "4도"] else 0)
                        if new_color != specs["colors_main"][i]:
                            specs["colors_main"][i] = new_color
                            update_current_project_data('book_specs', specs)
                
                c_add, c_del = st.columns([1, 1])
                with c_add:
                    if st.button("➕ 본문 도수 추가"):
                        specs["colors_main"].append("1도")
                        update_current_project_data('book_specs', specs)
                        st.rerun()
                with c_del:
                    if st.button("➖ 본문 도수 삭제"):
                        if len(specs["colors_main"]) > 1:
                            specs["colors_main"].pop()
                            update_current_project_data('book_specs', specs)
                            st.rerun()
                        else:
                            st.toast("⚠️ 최소 1개의 도수는 유지해야 합니다.")

                st.markdown("---")
                new_sol_color = st.radio("해설", ["1도", "2도", "4도"], key="color_sol", horizontal=True, index=["1도", "2도", "4도"].index(specs.get("colors_sol", "1도")))
                if new_sol_color != specs.get("colors_sol"):
                    specs["colors_sol"] = new_sol_color
                    update_current_project_data('book_specs', specs)

                st.markdown("---")
                st.markdown("#### 기타 옵션")
                col_opt1, col_opt2, col_opt3 = st.columns(3)
                with col_opt1:
                    is_ebook = st.checkbox("e-book 제작", value=specs.get("is_ebook", False))
                    if is_ebook != specs.get("is_ebook"):
                        specs["is_ebook"] = is_ebook
                        update_current_project_data('book_specs', specs)
                with col_opt2:
                    is_av = st.checkbox("한눈답", value=specs.get("is_answer_view", False))
                    if is_av != specs.get("is_answer_view"):
                        specs["is_answer_view"] = is_av
                        update_current_project_data('book_specs', specs)
                with col_opt3:
                    is_ap = st.checkbox("한눈답 (PDF만)", value=specs.get("is_answer_pdf", False))
                    if is_ap != specs.get("is_answer_pdf"):
                        specs["is_answer_pdf"] = is_ap
                        update_current_project_data('book_specs', specs)

    # ==========================================
    # [2. 개발 일정]
    # ==========================================
    elif menu == "2. 개발 일정":
        st.title("🗓️ 개발 일정 관리")
        
        with st.container(border=True):
            st.subheader("🛠️ 일정 생성 및 가져오기")
            col_date, col_actions = st.columns([1, 2])
            
            with col_date:
                schedule_date = get_schedule_date(current_p)
                default_date = schedule_date if schedule_date else current_p.get('target_date_val', datetime.today())
                target_date = st.date_input("기준일 (최종 플루토 OK)", default_date)
                if target_date != default_date:
                     update_current_project_data('target_date_val', target_date)
            
            with col_actions:
                c_btn1, c_btn2, c_btn3 = st.columns(3)
                with c_btn1:
                    if st.button("⚡ 자동 일정 생성", type="primary", help="기준일을 바탕으로 표준 일정을 자동 생성합니다."):
                         schedule_df = create_initial_schedule(target_date)
                         update_current_project_data('schedule_data', schedule_df)
                         st.rerun()
                with c_btn2:
                    sample_data = [
                        {"구분": "샘플 일정(일반)", "시작일": "2025-01-01", "종료일": "2025-01-05", "비고": "예시", "독립 일정": False, "주요 일정": "X"},
                        {"구분": "샘플 일정(중요)", "시작일": "2025-02-01", "종료일": "2025-02-05", "비고": "홈화면 노출", "독립 일정": False, "주요 일정": "O"}
                    ]
                    df_sample = pd.DataFrame(sample_data)
                    csv_sample = df_sample.to_csv(index=False).encode('utf-8-sig')
                    st.download_button(
                        label="⬇️ 양식 다운로드(엑셀)",
                        data=csv_sample,
                        file_name="일정표_양식.csv",
                        mime="text/csv"
                    )
                with c_btn3:
                     df_ics = current_p.get('schedule_data', pd.DataFrame())
                     if not df_ics.empty:
                        ics_data = create_ics_file(ensure_data_types(df_ics), current_p['title'])
                        st.download_button(
                            label="⬇️ ICS 파일 저장",
                            data=ics_data,
                            file_name=f"{current_p['series']}_{current_p['title']}_Schedule.ics",
                            mime="text/calendar"
                        )

            with st.expander("📂 일정표 업로드 (엑셀/CSV)", expanded=False):
                st.info("💡 '구분', '시작일', '종료일' 컬럼 필수. '주요 일정' 컬럼에 'O'를 입력하면 홈 화면에 노출됩니다.")
                uploaded_file = st.file_uploader("파일 선택", type=["xlsx", "xls", "csv"], label_visibility="collapsed")
                if uploaded_file:
                    if st.button("이 파일로 일정 덮어쓰기"):
                        try:
                            if uploaded_file.name.endswith('.csv'): 
                                df_new = pd.read_csv(uploaded_file)
                            else: 
                                df_new = pd.read_excel(uploaded_file)
                            
                            if '구분' in df_new.columns:
                                 target_year = int(current_p.get('year', datetime.now().year))
                                 for col in ['시작일', '종료일']:
                                     if col in df_new.columns:
                                         df_new[col] = df_new[col].apply(clean_korean_date)
                                         df_new[col] = pd.to_datetime(df_new[col], errors='coerce')
                                         df_new[col] = df_new[col].apply(lambda x: x.replace(year=target_year) if pd.notnull(x) and x.year == 1900 else x)

                                 if '소요 일수' not in df_new.columns and '시작일' in df_new.columns and '종료일' in df_new.columns:
                                     df_new['소요 일수'] = (df_new['종료일'] - df_new['시작일']).dt.days + 1
                                 
                                 if '선택' not in df_new.columns: df_new['선택'] = False
                                 if '독립 일정' not in df_new.columns: df_new['독립 일정'] = False
                                 if '비고' not in df_new.columns: df_new['비고'] = ""
                                 
                                 def mark_important_row(row):
                                     name = str(row['구분'])
                                     is_important = False
                                     if '주요 일정' in row.index:
                                         val = str(row['주요 일정']).strip().upper()
                                         if val in ['O', 'TRUE', 'YES', 'V']: is_important = True
                                     
                                     if not is_important:
                                         IMPORTANT_KEYWORDS = ["발주 회의", "집필 (본문 개발)", "1차 외부/교차 검토", "2차 외부/교차 검토", "3차 외부/교차 검토", "가쇄본 제작", "집필자 최종 검토", "내용 OK", "최종 플루토 OK", "플루토"]
                                         if any(k in name for k in IMPORTANT_KEYWORDS): is_important = True
                                     
                                     if is_important and not name.startswith("🔴"): return f"🔴 {name}"
                                     return name

                                 df_new['구분'] = df_new.apply(mark_important_row, axis=1)

                                 try:
                                     pluto_mask = df_new['구분'].astype(str).str.contains("플루토", na=False) 
                                     if pluto_mask.any():
                                         pluto_date = df_new.loc[pluto_mask, '종료일'].values[-1] 
                                         if pd.notnull(pluto_date):
                                            update_current_project_data('target_date_val', pd.to_datetime(pluto_date))
                                            st.toast("📅 '플루토' 관련 일정이 기준일로 동기화되었습니다.")
                                 except Exception as e: pass 

                                 if '주요 일정' in df_new.columns: df_new = df_new.drop(columns=['주요 일정'])

                                 update_current_project_data('schedule_data', df_new)
                                 st.success("일정이 성공적으로 업데이트되었습니다.")
                                 st.rerun()
                            else:
                                st.error("파일에 '구분' 컬럼이 없습니다.")
                        except Exception as e:
                            st.error(f"파일 처리 실패: {e}")

        df = current_p.get('schedule_data', pd.DataFrame())
        df = ensure_data_types(df) 

        st.sidebar.subheader("🛠️ 일정 조작")
        col_s1, col_s2 = st.sidebar.columns(2)
        trigger_rerun = False
        with col_s1:
            if st.button("⬆️ 위로"):
                indices = list(df.index); selected = df[df["선택"] == True].index.tolist()
                for idx in selected:
                    if idx > 0: indices[idx], indices[idx-1] = indices[idx-1], indices[idx]
                df = df.iloc[indices].reset_index(drop=True); update_current_project_data('schedule_data', df); trigger_rerun = True
        with col_s2:
            if st.button("⬇️ 아래로"):
                indices = list(df.index); selected = df[df["선택"] == True].index.tolist()
                for idx in reversed(selected):
                    if idx < len(df) - 1: indices[idx], indices[idx+1] = indices[idx+1], indices[idx]
                df = df.iloc[indices].reset_index(drop=True); update_current_project_data('schedule_data', df); trigger_rerun = True
        if st.sidebar.button("🗑️ 선택 삭제"):
            df = df[df["선택"] == False].reset_index(drop=True); update_current_project_data('schedule_data', df); trigger_rerun = True
        
        st.sidebar.markdown("---")
        if st.sidebar.button("🚀 전체 재계산 (독립일정 제외)", type="primary"):
            target = current_p.get('target_date_val', datetime.today())
            final_df = recalculate_dates(df, target); update_current_project_data('schedule_data', final_df); trigger_rerun = True

        if trigger_rerun: st.rerun()

        edited_df = st.data_editor(
            df, num_rows="dynamic", hide_index=True, key="schedule_editor",
            column_order=["선택", "독립 일정", "구분", "소요 일수", "시작일", "종료일", "비고"],
            column_config={
                "시작일": st.column_config.DateColumn("시작일", format="YYYY-MM-DD dddd"),
                "종료일": st.column_config.DateColumn("종료일", format="YYYY-MM-DD dddd"),
            }
        )

        if not edited_df.equals(df):
             for index, row in edited_df.iterrows():
                if row['독립 일정']:
                    try:
                        s_date = pd.to_datetime(row['시작일']).date() if pd.notnull(row['시작일']) else None
                        duration = int(row['소요 일수'])
                        if s_date and duration >= 0:
                            new_end = s_date + timedelta(days=duration - 1)
                            edited_df.at[index, '종료일'] = new_end
                    except: pass
             update_current_project_data('schedule_data', ensure_data_types(edited_df))

    # ==========================================
    # [3. 참여자]
    # ==========================================
    elif menu == "3. 참여자":
        st.title("👥 참여자 관리")
        tab_auth, tab_rev, tab_partner = st.tabs(["📝 집필진", "🔍 검토진", "🏢 참여업체"])

        def get_selected_row(df, selection):
            if selection.selection.rows:
                return df.iloc[selection.selection.rows[0]].to_dict(), selection.selection.rows[0]
            return None, None

        # --- 1. 집필진 ---
        with tab_auth:
            st.info("💡 목록에서 행을 클릭하면 수정/삭제할 수 있습니다.")
            auth_df = pd.DataFrame(current_p.get('author_list', []))
            cols = ["이름", "학교급", "소속", "과목", "역할", "연락처", "이메일", "우편번호", "주소", "상세주소", "은행명", "계좌번호", "주민번호(앞)"]
            if auth_df.empty: auth_df = pd.DataFrame(columns=cols)
            else:
                for c in cols:
                    if c not in auth_df.columns: auth_df[c] = ""

            st.markdown("##### 📋 집필진 목록")
            selection = st.dataframe(
                auth_df[cols], 
                on_select="rerun", 
                selection_mode="single-row", 
                use_container_width=True,
                key="auth_table_select"
            )
            selected_row, selected_idx = get_selected_row(auth_df, selection)

            st.write("---")
            form_title = f"✏️ 집필진 정보 수정 ({selected_row['이름']})" if selected_row else "➕ 신규 집필진 등록"
            k_suffix = f"_{selected_idx}" if selected_idx is not None else "_new"

            with st.form("author_form", clear_on_submit=False, border=True):
                st.subheader(form_title)
                def val(k, d=""): return selected_row.get(k, d) if selected_row else d

                col1, col2, col3, col4, col5 = st.columns([1, 1, 1.5, 1.5, 1.2])
                with col1: name = st.text_input("이름 *", value=val("이름"), key=f"auth_name{k_suffix}")
                with col2: school = st.selectbox("학교급", ["초등", "중학", "고교"], index=["초등", "중학", "고교"].index(val("학교급", "초등")) if val("학교급") in ["초등", "중학", "고교"] else 0, key=f"auth_school{k_suffix}")
                with col3: affil = st.text_input("소속", value=val("소속"), key=f"auth_affil{k_suffix}")
                with col4: subj = st.selectbox("담당 과목", ["물리학", "화학", "생명과학", "지구과학", "공통", "기타"], index=["물리학", "화학", "생명과학", "지구과학", "공통", "기타"].index(val("과목", "공통")) if val("과목") in ["물리학", "화학", "생명과학", "지구과학", "공통", "기타"] else 4, key=f"auth_subj{k_suffix}")
                with col5: role = st.radio("역할", ["대표집필", "공동집필"], horizontal=True, index=["대표집필", "공동집필"].index(val("역할", "공동집필")) if val("역할") in ["대표집필", "공동집필"] else 1, key=f"auth_role{k_suffix}")
                
                col_b1, col_b2 = st.columns(2)
                with col_b1: phone = st.text_input("휴대전화", value=val("연락처"), key=f"auth_phone{k_suffix}")
                with col_b2: email = st.text_input("이메일", value=val("이메일"), key=f"auth_email{k_suffix}")
                
                with st.expander("배송 및 정산 정보"):
                    c_zip, c_btn, c_addr = st.columns([1.2, 0.8, 3])
                    with c_zip: zipcode = st.text_input("우편번호", value=val("우편번호"), key=f"auth_zip{k_suffix}")
                    with c_btn:
                        st.markdown(" ") 
                        st.markdown(" ")
                        st.link_button("🔍 검색", "https://www.juso.go.kr/support/AddressMainSearch.do?searchType=TOTAL")
                    with c_addr: addr = st.text_input("주소", value=val("주소"), key=f"auth_addr{k_suffix}")
                    detail = st.text_input("상세주소", value=val("상세주소"), key=f"auth_detail{k_suffix}")
                    d1, d2, d3 = st.columns([1, 2, 1])
                    bank = st.text_input("은행명", value=val("은행명"), key=f"auth_bank{k_suffix}")
                    account = st.text_input("계좌번호", value=val("계좌번호"), key=f"auth_acc{k_suffix}")
                    rid = st.text_input("주민번호(앞)", value=val("주민번호(앞)"), key=f"auth_rid{k_suffix}")
                
                c_btn1, c_btn2 = st.columns([1, 1])
                with c_btn1:
                    if st.form_submit_button("💾 저장 / 등록", type="primary"):
                        if not name: st.error("이름 필수")
                        else:
                            new_data = {"이름": name, "학교급": school, "소속": affil, "과목": subj, "역할": role, "연락처": phone, "이메일": email, "우편번호": zipcode, "주소": addr, "상세주소": detail, "은행명": bank, "계좌번호": account, "주민번호(앞)": rid}
                            if selected_row: current_p['author_list'][selected_idx] = new_data; st.success("수정 완료")
                            else: current_p['author_list'].append(new_data); st.success("등록 완료")
                            st.rerun()
                with c_btn2:
                    if selected_row and st.form_submit_button("🗑️ 삭제", type="secondary"):
                        del current_p['author_list'][selected_idx]
                        st.warning("삭제 완료")
                        st.rerun()

        # --- 2. 검토진 ---
        with tab_rev:
            st.info("💡 목록에서 행을 클릭하면 수정/삭제할 수 있습니다.")
            part_df = pd.DataFrame(current_p.get('reviewer_list', []))
            cols = ["이름", "학교급", "소속", "과목", "검토차수", "매칭정보", "연락처", "이메일", "우편번호", "주소", "상세주소", "은행명", "계좌번호", "주민번호(앞)"]
            if part_df.empty: part_df = pd.DataFrame(columns=cols)
            else: 
                for c in cols: 
                    if c not in part_df.columns: part_df[c] = ""

            st.markdown("##### 📋 검토진 목록")
            selection = st.dataframe(
                part_df[cols], 
                on_select="rerun", 
                selection_mode="single-row", 
                use_container_width=True,
                key="rev_table_select"
            )
            selected_row, selected_idx = get_selected_row(part_df, selection)

            st.write("---")
            form_title = f"✏️ 검토진 정보 수정 ({selected_row['이름']})" if selected_row else "➕ 신규 검토진 등록"
            k_suffix = f"_{selected_idx}" if selected_idx is not None else "_new"

            with st.form("rev_form", clear_on_submit=False, border=True):
                st.subheader(form_title)
                def val(k, d=""): return selected_row.get(k, d) if selected_row else d

                col1, col2, col3, col4, col5 = st.columns([1, 1, 1.5, 1.5, 1.2])
                with col1: f_name = st.text_input("이름", value=val("이름"), key=f"rev_name{k_suffix}")
                with col2: f_school = st.selectbox("학교급", ["초등", "중학", "고교"], index=["초등", "중학", "고교"].index(val("학교급", "초등")) if val("학교급") in ["초등", "중학", "고교"] else 0, key=f"rev_school{k_suffix}")
                with col3: f_affil = st.text_input("소속", value=val("소속"), key=f"rev_affil{k_suffix}")
                with col4: f_subj = st.selectbox("담당 과목", ["물리학", "화학", "생명과학", "지구과학", "공통", "기타"], index=["물리학", "화학", "생명과학", "지구과학", "공통", "기타"].index(val("과목", "공통")) if val("과목") in ["물리학", "화학", "생명과학", "지구과학", "공통", "기타"] else 4, key=f"rev_subj{k_suffix}")
                with col5: 
                    role_opts = ["1차 외부검토", "2차 외부검토", "3차 외부검토", "편집검토", "감수", "직접 입력"]
                    curr_role = val("검토차수")
                    idx = role_opts.index(curr_role) if curr_role in role_opts else 5
                    f_role_sel = st.selectbox("검토 차수", role_opts, index=idx, key=f"rev_role_sel{k_suffix}")
                    f_role_input = st.text_input("검토 차수 (직접 입력)", value=curr_role if f_role_sel == "직접 입력" else "", key=f"rev_role_inp{k_suffix}")

                col_b1, col_b2 = st.columns(2)
                with col_b1: f_phone = st.text_input("휴대전화", value=val("연락처"), key=f"rev_phone{k_suffix}")
                with col_b2: f_email = st.text_input("이메일", value=val("이메일"), key=f"rev_email{k_suffix}")

                st.write("###### 🔗 검토 범위 설정 (매칭 정보)")
                plan_df = current_p.get('planning_data', pd.DataFrame())
                
                if plan_df.empty:
                    st.warning("⚠️ '1. 교재 기획' 메뉴에서 배열표를 먼저 업로드해주세요.")
                    match_val_default = val("매칭정보")
                    st.text_area("매칭 정보 (직접 입력)", value=match_val_default, disabled=True, key=f"rev_match_disp{k_suffix}")
                    final_match_val = match_val_default
                else:
                    plan_df['UnitKey'] = plan_df.apply(lambda x: f"[{x.get('분권','')}] {x.get('대단원','')} > {x.get('중단원','')}", axis=1)
                    all_units = plan_df['UnitKey'].unique().tolist()
                    
                    author_map = {}
                    if '집필자' in plan_df.columns:
                        for auth in plan_df['집필자'].unique():
                            if pd.notnull(auth) and str(auth).strip() not in ['-', '']:
                                author_map[auth] = plan_df[plan_df['집필자'] == auth]['UnitKey'].tolist()
                    
                    big_unit_map = {}
                    if '대단원' in plan_df.columns:
                         for big in plan_df['대단원'].unique():
                             if pd.notnull(big) and str(big).strip() != "":
                                 big_unit_map[big] = plan_df[plan_df['대단원'] == big]['UnitKey'].tolist()

                    match_tab1, match_tab2, match_tab3 = st.tabs(["🙋‍♂️ 집필자 기준", "📚 대단원 기준", "🎯 개별 단원 선택"])
                    selected_units = []
                    current_match_str = val("매칭정보")
                    pre_selected = [x.strip() for x in current_match_str.split(',')] if current_match_str else []

                    with match_tab1:
                        st.caption("선택한 집필자가 작성한 모든 단원을 자동으로 선택합니다.")
                        authors = list(author_map.keys())
                        sel_authors = st.multiselect("집필자 선택", authors, key=f"match_auth_sel{k_suffix}")
                        if sel_authors:
                            for a in sel_authors: selected_units.extend(author_map.get(a, []))

                    with match_tab2:
                        st.caption("선택한 대단원에 포함된 모든 중단원을 자동으로 선택합니다.")
                        big_units = list(big_unit_map.keys())
                        sel_bigs = st.multiselect("대단원 선택", big_units, key=f"match_big_sel{k_suffix}")
                        if sel_bigs:
                            for b in sel_bigs: selected_units.extend(big_unit_map.get(b, []))

                    with match_tab3:
                        st.caption("원하는 단원을 직접 선택합니다.")
                        valid_pre = [u for u in pre_selected if u in all_units]
                        sel_manual = st.multiselect("단원 선택", all_units, default=valid_pre, key=f"match_manual_sel{k_suffix}")
                        if sel_manual: selected_units.extend(sel_manual)
                    
                    final_units = sorted(list(set(selected_units)))
                    
                    if final_units:
                        st.success(f"총 {len(final_units)}개 단원이 선택되었습니다.")
                        with st.expander("선택된 단원 목록 확인"): st.write(final_units)
                        final_match_val = ", ".join(final_units)
                    else:
                        if not selected_units and current_match_str:
                             st.info(f"기존 설정 유지: {current_match_str}")
                             final_match_val = current_match_str
                        else:
                             st.caption("선택된 검토 범위가 없습니다.")
                             final_match_val = ""

                with st.expander("배송 및 정산 정보"):
                    c_zip, c_btn, c_addr = st.columns([1.2, 0.8, 3])
                    with c_zip: zipcode = st.text_input("우편번호", value=val("우편번호"), key=f"rev_zip{k_suffix}")
                    with c_btn:
                        st.markdown(" ") 
                        st.markdown(" ")
                        st.link_button("🔍 검색", "https://www.juso.go.kr/support/AddressMainSearch.do?searchType=TOTAL")
                    with c_addr: addr = st.text_input("주소", value=val("주소"), key=f"rev_addr{k_suffix}")
                    detail = st.text_input("상세주소", value=val("상세주소"), key=f"rev_detail{k_suffix}")
                    d1, d2, d3 = st.columns([1, 2, 1])
                    bank = st.text_input("은행명", value=val("은행명"), key=f"rev_bank{k_suffix}")
                    acc = st.text_input("계좌번호", value=val("계좌번호"), key=f"rev_acc{k_suffix}")
                    rid = st.text_input("주민번호(앞)", value=val("주민번호(앞)"), key=f"rev_rid{k_suffix}")

                c_btn1, c_btn2 = st.columns([1, 1])
                with c_btn1:
                    if st.form_submit_button("💾 저장 / 등록", type="primary"):
                        final_role = f_role_input if f_role_sel == "직접 입력" else f_role_sel
                        if not f_name or not final_role: st.error("이름/차수 필수")
                        else:
                            role_clean = normalize_string(final_role)
                            new_data = {"이름": f_name, "검토차수": role_clean, "매칭정보": final_match_val, "소속": f_affil, "학교급": f_school, "과목": f_subj, "연락처": f_phone, "이메일": f_email, "우편번호": zipcode, "주소": addr, "상세주소": detail, "은행명": bank, "계좌번호": acc, "주민번호(앞)": rid}
                            
                            if selected_row: current_p['reviewer_list'][selected_idx] = new_data; st.success("수정 완료")
                            else: current_p['reviewer_list'].append(new_data); st.success("등록 완료")
                            
                            rev_std = current_p['review_standards']
                            if role_clean and role_clean not in rev_std['구분'].apply(normalize_string).values:
                                new_std = pd.DataFrame([{"구분": role_clean, "단가(쪽)": 0, "단가(문항)": 0}])
                                current_p['review_standards'] = pd.concat([rev_std, new_std], ignore_index=True)
                            dev_df = current_p['dev_data']
                            if role_clean and role_clean not in dev_df.columns:
                                dev_df[role_clean] = "-"
                                current_p['dev_data'] = dev_df
                            st.rerun()
                with c_btn2:
                    if selected_row and st.form_submit_button("🗑️ 삭제", type="secondary"):
                        del current_p['reviewer_list'][selected_idx]
                        st.warning("삭제 완료")
                        st.rerun()

        # --- 3. 참여업체 ---
        with tab_partner:
            st.info("💡 목록에서 행을 클릭하면 수정/삭제할 수 있습니다.")
            part_df = pd.DataFrame(current_p.get('partner_list', []))
            cols = ["업체명", "분야", "담당자", "연락처", "이메일", "비고"]
            if part_df.empty: part_df = pd.DataFrame(columns=cols)
            else: 
                for c in cols: 
                    if c not in part_df.columns: part_df[c] = ""

            st.markdown("##### 📋 협력 업체 목록")
            selection = st.dataframe(
                part_df[cols], 
                on_select="rerun", 
                selection_mode="single-row", 
                use_container_width=True,
                key="part_table_select"
            )
            selected_row, selected_idx = get_selected_row(part_df, selection)

            st.write("---")
            form_title = f"✏️ 업체 정보 수정 ({selected_row['업체명']})" if selected_row else "➕ 신규 업체 등록"
            k_suffix = f"_{selected_idx}" if selected_idx is not None else "_new"

            with st.form("partner_form", clear_on_submit=False, border=True):
                st.subheader(form_title)
                def val(k, d=""): return selected_row.get(k, d) if selected_row else d

                col_p1, col_p2 = st.columns(2)
                with col_p1: p_name = st.text_input("업체명 *", value=val("업체명"), key=f"part_name{k_suffix}")
                with col_p2: 
                    default_types = val("분야").split(", ") if val("분야") else []
                    default_types = [t for t in default_types if t in ["편집", "표지", "인쇄", "사진", "가쇄본"]]
                    p_types = st.multiselect("참여 분야 (선택)", ["편집", "표지", "인쇄", "사진", "가쇄본"], default=default_types, key=f"part_types{k_suffix}")
                    p_type_direct = st.text_input("참여 분야 (직접 입력)", value="", key=f"part_type_dir{k_suffix}")
                col_p3, col_p4, col_p5 = st.columns(3)
                with col_p3: p_person = st.text_input("담당자명", value=val("담당자"), key=f"part_person{k_suffix}")
                with col_p4: p_contact = st.text_input("연락처", value=val("연락처"), key=f"part_contact{k_suffix}")
                with col_p5: p_email = st.text_input("이메일", value=val("이메일"), key=f"part_email{k_suffix}")
                p_note = st.text_area("비고", value=val("비고"), key=f"part_note{k_suffix}")
                
                c_btn1, c_btn2 = st.columns([1, 1])
                with c_btn1:
                    if st.form_submit_button("💾 저장 / 등록", type="primary"):
                        if not p_name: st.error("업체명 필수")
                        else:
                            final_roles = ", ".join(p_types + ([p_type_direct] if p_type_direct else []))
                            new_data = {"업체명": p_name, "분야": final_roles, "담당자": p_person, "연락처": p_contact, "이메일": p_email, "비고": p_note}
                            if selected_row: current_p['partner_list'][selected_idx] = new_data; st.success("수정 완료")
                            else: current_p['partner_list'].append(new_data); st.success("등록 완료")
                            st.rerun()
                with c_btn2:
                    if selected_row and st.form_submit_button("🗑️ 삭제", type="secondary"):
                        del current_p['partner_list'][selected_idx]
                        st.warning("삭제 완료")
                        st.rerun()

    # ==========================================
    # [4. 개발 프로세스]
    # ==========================================
    elif menu == "4. 개발 프로세스":
        st.title("⚙️ 개발 프로세스 관리")
        tab_status, tab_detail, tab_progress = st.tabs(["참여자 배정", "상세 진행 관리", "진행 상황"])
        
        with tab_status:
            col_title, col_btn = st.columns([4, 1.5])
            with col_title:
                st.markdown("##### 📝 단원별 집필/검토자 배정 매트릭스")
            with col_btn:
                if st.button("🔄 검토자 자동 배정 (초기화 후 재배정)", type="primary"):
                    dev_df = current_p['dev_data']
                    review_cols = [c for c in dev_df.columns if "검토" in c or "감수" in c]
                    for col in review_cols:
                        if col not in ["검토상태", "검토완료"]: dev_df[col] = "-"
                    
                    cnt = 0
                    for r in current_p['reviewer_list']:
                        match_targets = [t.strip() for t in str(r.get('매칭정보','')).split(',') if t.strip()]
                        role_col = normalize_string(r.get('검토차수'))
                        
                        if role_col in dev_df.columns and match_targets:
                            for idx, row in dev_df.iterrows():
                                unit_name = str(row['단원명'])
                                unit_match_exact = unit_name in match_targets
                                
                                unit_match_contains = False
                                for target in match_targets:
                                    if target in unit_name or unit_name in target:
                                        unit_match_contains = True
                                        break
                                
                                author_match = any(t == str(row['집필자']) for t in match_targets)
                                
                                if unit_match_exact or unit_match_contains or author_match:
                                    current_val = str(dev_df.at[idx, role_col])
                                    if current_val in ["-", "", "nan", "None"]: 
                                        dev_df.at[idx, role_col] = r['이름']; cnt += 1
                                    elif r['이름'] not in current_val: 
                                        dev_df.at[idx, role_col] = current_val + ", " + r['이름']; cnt += 1

                    current_p['dev_data'] = dev_df
                    st.success(f"기존 배정을 초기화하고, {cnt}건의 매칭을 새로 완료했습니다!")
                    st.rerun()

            dev_df = current_p['dev_data']
            base_cols = ["단원명", "집필자"]
            desired_order = ["1차외부검토", "2차외부검토", "3차외부검토", "편집검토", "감수"]
            sorted_review_cols = [c for c in desired_order if c in dev_df.columns]
            other_cols = [c for c in dev_df.columns if ("검토" in c or "감수" in c) and c not in ["검토상태", "검토자", "검토료_단가"] and c not in ["집필완료", "검토완료", "피드백완료", "디자인완료"] and c not in sorted_review_cols]
            final_cols = base_cols + sorted_review_cols + other_cols
            
            edited = st.data_editor(dev_df[final_cols], hide_index=True, key="dev_process_matrix_editor")
            if not edited.equals(dev_df[final_cols]):
                dev_df.update(edited)
                current_p['dev_data'] = dev_df

        with tab_detail:
             st.markdown("##### ✍️ 상세 집필/검토/디자인 상태 관리 (체크하여 완료 표시)")
             dev_df = current_p['dev_data']
             status_cols = ["단원명", "집필자", "집필완료", "검토완료", "피드백완료", "디자인완료", "비고"]
             valid_status_cols = [c for c in status_cols if c in dev_df.columns]
             
             edited_status = st.data_editor(
                 dev_df[valid_status_cols], 
                 hide_index=True, 
                 key="dev_status_editor",
                 column_config={
                    "집필완료": st.column_config.CheckboxColumn("집필", width="small"),
                    "검토완료": st.column_config.CheckboxColumn("검토", width="small"),
                    "피드백완료": st.column_config.CheckboxColumn("피드백", width="small"),
                    "디자인완료": st.column_config.CheckboxColumn("디자인", width="small"),
                 }
             )
             if not edited_status.equals(dev_df[valid_status_cols]):
                 dev_df.update(edited_status)
                 current_p['dev_data'] = dev_df
                 st.rerun()

        with tab_progress:
            st.markdown("##### 🚀 전체 일정 진행 대시보드")
            schedule_df = current_p.get('schedule_data', pd.DataFrame())
            if not schedule_df.empty:
                schedule_df = ensure_data_types(schedule_df)
                pre_ok_df = schedule_df[schedule_df['구분'].str.contains("최종 플루토 OK", na=False) == False]
                
                total_tasks = len(pre_ok_df)
                today = datetime.now().date()
                completed_tasks = pre_ok_df[pre_ok_df['종료일'] < today]
                completed_count = len(completed_tasks)
                progress = completed_count / total_tasks if total_tasks > 0 else 0.0
                
                st.metric("전체 진행률 (플루토 OK 전)", f"{int(progress * 100)}%", delta_color="off")
                st.progress(progress)
                st.markdown("### 🚦 단계별 상태")
                
                sorted_schedule = schedule_df.sort_values('시작일')
                for _, row in sorted_schedule.iterrows():
                    try:
                        is_completed = False
                        is_ongoing = False
                        s_date = row.get('시작일')
                        e_date = row.get('종료일')
                        if pd.notnull(e_date):
                            if e_date < today: is_completed = True
                            elif pd.notnull(s_date) and s_date <= today <= e_date: is_ongoing = True
                        
                        status = "✅ 완료" if is_completed else ("🏃 진행중" if is_ongoing else "⚪ 대기")
                        if row['구분'].startswith("🔴"):
                             st.error(f"**{status}** | **{row['구분'].replace('🔴 ','')}** ({row['시작일']} ~ {row['종료일']})")
                        else:
                             st.write(f"**{status}** | {row['구분']} ({row['시작일']} ~ {row['종료일']})")
                    except: continue
            else: st.info("등록된 일정이 없습니다.")

    # ==========================================
    # [5. 결과보고서 및 정산]
    # ==========================================
    elif menu == "5. 결과보고서 및 정산":
        st.title("📑 결과보고서 및 정산")
        tab_report, tab_settle = st.tabs(["결과보고서", "정산"])
        
        with tab_report:
            st.markdown("##### 📎 필수 서류 구비 체크리스트")
            checklist_df = current_p.get('report_checklist', pd.DataFrame())
            edited_checklist = st.data_editor(checklist_df, hide_index=True, num_rows="fixed", key="report_checklist_editor")
            if not edited_checklist.equals(checklist_df):
                update_current_project_data('report_checklist', edited_checklist)
                st.rerun()

        with tab_settle:
            st.subheader("1. 기준 단가 설정")
            col_set1, col_set2 = st.columns(2)
            
            with col_set1:
                st.markdown("###### ✍️ 집필료 기준")
                auth_std_df = current_p['author_standards']
                # Data Editor for Author Standards (2 rows)
                edited_auth_std = st.data_editor(
                    auth_std_df, 
                    num_rows="fixed", 
                    hide_index=True, 
                    key="auth_std_editor",
                    column_config={
                        "구분": st.column_config.TextColumn("구분", disabled=True),
                        "원고료": st.column_config.NumberColumn("원고료(단가)", format="%d원"),
                        "검토료": st.column_config.NumberColumn("검토료(단가)", format="%d원")
                    }
                )
                if not edited_auth_std.equals(auth_std_df):
                    update_current_project_data('author_standards', edited_auth_std); st.rerun()

            with col_set2:
                st.markdown("###### 🔍 검토료 기준")
                rev_std_df = current_p.get('review_standards', pd.DataFrame())
                # [수정] 지급기준 열 제거 및 UI 정리
                edited_rev_std = st.data_editor(
                    rev_std_df, 
                    num_rows="dynamic", 
                    hide_index=True, 
                    key="rev_std_editor",
                    column_order=["구분", "단가(쪽)", "단가(문항)"], # 지급기준 열 제외
                    column_config={
                        "구분": st.column_config.TextColumn("구분"),
                        "단가(쪽)": st.column_config.NumberColumn("단가(쪽)", format="%d원"),
                        "단가(문항)": st.column_config.NumberColumn("단가(문항)", format="%d원")
                    }
                )
                if not edited_rev_std.equals(rev_std_df):
                    update_current_project_data('review_standards', edited_rev_std); st.rerun()

            st.markdown("---")
            st.subheader("2. 정산 내역서")

            # [Logic] Auto Mode
            def generate_auto_data():
                plan_df = current_p.get('planning_data', pd.DataFrame())
                dev_df = current_p.get('dev_data', pd.DataFrame())
                
                if not plan_df.empty:
                    if '쪽수' not in plan_df.columns: plan_df['쪽수'] = 0
                    if '문항수' not in plan_df.columns: plan_df['문항수'] = 0
                    plan_df['쪽수_calc'] = pd.to_numeric(plan_df['쪽수'], errors='coerce').fillna(0.0)
                    plan_df['문항수_calc'] = pd.to_numeric(plan_df['문항수'], errors='coerce').fillna(0.0)
                
                new_rows = []
                auth_std = current_p['author_standards']
                def get_auth_price(unit_type, price_type):
                    try:
                        row = auth_std[auth_std['구분'] == unit_type + "당"]
                        if not row.empty:
                            val = row.iloc[0][price_type]
                            return int(val) if pd.notnull(val) else 0
                    except: pass
                    return 0

                # Author Rows
                if not plan_df.empty and '집필자' in plan_df.columns:
                    auth_grouped = plan_df.groupby('집필자')[['쪽수_calc', '문항수_calc']].sum().reset_index()
                    for _, row in auth_grouped.iterrows():
                        name = row['집필자']
                        if name in ['-', '', 'nan', 'None']: continue
                        if row['쪽수_calc'] > 0:
                            price = get_auth_price("쪽", "원고료")
                            new_rows.append({"구분": "집필", "이름": name, "내용": "원고 집필 (쪽)", "지급기준": "쪽당", "수량": row['쪽수_calc'], "단가": price, "비고": ""})
                        if row['문항수_calc'] > 0:
                            price = get_auth_price("문항", "원고료")
                            new_rows.append({"구분": "집필", "이름": name, "내용": "원고 집필 (문항)", "지급기준": "문항당", "수량": row['문항수_calc'], "단가": price, "비고": ""})

                # Reviewer Rows
                if not dev_df.empty:
                    unit_stats = {}
                    if not plan_df.empty:
                         for _, r in plan_df.iterrows():
                            uname = f"[{r.get('분권','')}] {r.get('대단원','')} > {r.get('중단원','')}"
                            unit_stats[uname] = {'page': r.get('쪽수_calc',0), 'item': r.get('문항수_calc',0)}
                    
                    rev_prices = {}
                    for _, r in rev_std_df.iterrows():
                        key = normalize_string(r['구분'])
                        rev_prices[key] = {'name': r['구분'], 'p_page': r.get('단가(쪽)',0), 'p_item': r.get('단가(문항)',0)}

                    reviewer_agg = {} 
                    for _, row in dev_df.iterrows():
                        uname = str(row.get('단원명',''))
                        stats = unit_stats.get(uname, {'page':0, 'item':0})
                        for col in dev_df.columns:
                            c_clean = normalize_string(col)
                            if c_clean in rev_prices:
                                cell = str(row[col])
                                if cell and cell not in ['-', '', 'nan', 'None']:
                                    people = [x.strip() for x in cell.split(',')]
                                    for p_name in people:
                                        if not p_name: continue
                                        key = (p_name, rev_prices[c_clean]['name'])
                                        if key not in reviewer_agg: reviewer_agg[key] = {'page':0, 'item':0}
                                        reviewer_agg[key]['page'] += stats['page']
                                        reviewer_agg[key]['item'] += stats['item']

                    for (r_name, r_role), stats in reviewer_agg.items():
                        role_key = normalize_string(r_role)
                        prices = rev_prices.get(role_key, {'p_page':0, 'p_item':0})
                        if stats['page'] > 0:
                            new_rows.append({"구분": "검토", "이름": r_name, "내용": f"{r_role} (쪽)", "지급기준": "쪽당", "수량": stats['page'], "단가": prices['p_page'], "비고": ""})
                        if stats['item'] > 0:
                             new_rows.append({"구분": "검토", "이름": r_name, "내용": f"{r_role} (문항)", "지급기준": "문항당", "수량": stats['item'], "단가": prices['p_item'], "비고": ""})
                return new_rows

            col_b1, col_b2, col_dummy = st.columns([1, 1, 3])
            with col_b1:
                if st.button("🔄 자동 산출 (데이터 연동)", type="primary"):
                    new_data = generate_auto_data()
                    current_p['settlement_list'] = new_data
                    st.rerun()
            with col_b2:
                if st.button("📝 직접 입력 (초기화)", type="secondary"):
                    current_p['settlement_list'] = [
                        {"구분": "집필", "이름": "", "내용": "", "지급기준": "쪽당", "수량": 0, "단가": 0, "비고": ""},
                        {"구분": "검토", "이름": "", "내용": "", "지급기준": "쪽당", "수량": 0, "단가": 0, "비고": ""}
                    ]
                    st.rerun()

            if 'settlement_list' not in current_p: current_p['settlement_list'] = []
            settle_df = pd.DataFrame(current_p['settlement_list'])
            if settle_df.empty: settle_df = pd.DataFrame(columns=["구분", "이름", "내용", "지급기준", "수량", "단가", "비고"])

            settle_df['수량'] = pd.to_numeric(settle_df['수량'], errors='coerce').fillna(0)
            settle_df['단가'] = pd.to_numeric(settle_df['단가'], errors='coerce').fillna(0)
            settle_df['공급가액'] = settle_df['수량'] * settle_df['단가']

            st.markdown("#### ✍️ 집필료 정산 내역")
            write_df = settle_df[settle_df['구분'] == '집필'].reset_index(drop=True)
            if write_df.empty: write_df = pd.DataFrame(columns=["구분", "이름", "내용", "지급기준", "수량", "단가", "공급가액", "비고"])
            
            edited_write = st.data_editor(
                write_df,
                num_rows="dynamic",
                column_order=["이름", "내용", "지급기준", "수량", "단가", "공급가액", "비고"],
                column_config={
                    "지급기준": st.column_config.SelectboxColumn("지급기준", options=["쪽당", "문항당", "건당(직접)", "식(직접)"]),
                    "수량": st.column_config.NumberColumn(format="%.1f"),
                    "단가": st.column_config.NumberColumn(format="%d원"),
                    "공급가액": st.column_config.NumberColumn(format="%d원", disabled=True),
                },
                key="settlement_write_editor"
            )

            st.markdown("#### 🔍 검토료 정산 내역")
            review_df = settle_df[settle_df['구분'] == '검토'].reset_index(drop=True)
            if review_df.empty: review_df = pd.DataFrame(columns=["구분", "이름", "내용", "지급기준", "수량", "단가", "공급가액", "비고"])

            edited_review = st.data_editor(
                review_df,
                num_rows="dynamic",
                column_order=["이름", "내용", "지급기준", "수량", "단가", "공급가액", "비고"],
                column_config={
                    "지급기준": st.column_config.SelectboxColumn("지급기준", options=["쪽당", "문항당", "건당(직접)", "식(직접)"]),
                    "수량": st.column_config.NumberColumn(format="%.1f"),
                    "단가": st.column_config.NumberColumn(format="%d원"),
                    "공급가액": st.column_config.NumberColumn(format="%d원", disabled=True),
                },
                key="settlement_review_editor"
            )

            if not edited_write.equals(write_df) or not edited_review.equals(review_df):
                edited_write['구분'] = '집필'
                edited_review['구분'] = '검토'
                
                cols_to_save = ["구분", "이름", "내용", "지급기준", "수량", "단가", "비고"]
                for c in cols_to_save:
                    if c not in edited_write.columns: edited_write[c] = ""
                    if c not in edited_review.columns: edited_review[c] = ""

                final_df = pd.concat([edited_write[cols_to_save], edited_review[cols_to_save]], ignore_index=True)
                other_df = settle_df[~settle_df['구분'].isin(['집필', '검토'])]
                if not other_df.empty: final_df = pd.concat([final_df, other_df[cols_to_save]], ignore_index=True)

                current_p['settlement_list'] = final_df.to_dict('records')
                st.rerun()
            
            total_write = edited_write['공급가액'].sum() if not edited_write.empty else 0
            total_review = edited_review['공급가액'].sum() if not edited_review.empty else 0
            
            c_t1, c_t2, c_t3 = st.columns(3)
            c_t1.metric("✍️ 집필료 합계", f"{int(total_write):,}원")
            c_t2.metric("🔍 검토료 합계", f"{int(total_review):,}원")
            c_t3.metric("💰 총 지급액 (공급가액)", f"{int(total_write + total_review):,}원")