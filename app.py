import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import re 
import uuid 
import io 
import os
import pickle
import base64
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
        print(f"구글 시트 연결 오류: {e}")
        return None

# [수정] 데이터 불러오기 로직 (분할된 셀 합치기)
def load_data_from_sheet():
    sheet = get_db_connection()
    if sheet:
        try:
            # A열(1번째 열)의 모든 데이터를 가져옵니다.
            col_values = sheet.col_values(1)
            if col_values:
                # 나눠진 문자열을 하나로 합칩니다.
                full_b64_str = "".join(col_values)
                binary_data = base64.b64decode(full_b64_str)
                return pickle.loads(binary_data)
        except Exception as e:
            print(f"데이터 로드 오류: {e}")
            pass
    return []

# [수정] 데이터 저장 로직 (5만 자 제한 우회 - 분할 저장)
def save_data_to_sheet(data):
    sheet = get_db_connection()
    if sheet:
        try:
            binary_data = pickle.dumps(data)
            b64_str = base64.b64encode(binary_data).decode('utf-8')
            
            # 구글 시트 셀 제한(50,000자)을 피하기 위해 45,000자 단위로 자릅니다.
            chunk_size = 45000
            chunks = [b64_str[i:i+chunk_size] for i in range(0, len(b64_str), chunk_size)]
            
            # 기존 데이터를 지웁니다 (찌꺼기 방지)
            sheet.clear()
            
            # 세로(열)로 저장하기 위해 2차원 리스트로 변환 [[chunk1], [chunk2], ...]
            update_values = [[chunk] for chunk in chunks]
            
            # A1 셀부터 순서대로 저장
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
            st.toast("☁️ 클라우드에서 데이터를 불러왔습니다.")
        else:
            st.session_state['projects'] = []
            # 로컬 백업 확인 (선택 사항)
            if os.path.exists("book_project_data.pkl"):
                 try:
                    with open("book_project_data.pkl", 'rb') as f:
                        st.session_state['projects'] = pickle.load(f)
                    st.toast("📂 로컬 백업 파일에서 데이터를 불러왔습니다.")
                 except: pass

for p in st.session_state['projects']:
    if 'created_at' not in p:
        p['created_at'] = datetime.now()

if 'current_project_id' not in st.session_state:
    st.session_state['current_project_id'] = None 
if 'selected_overview_id' not in st.session_state:
    st.session_state['selected_overview_id'] = None

def normalize_string(s):
    return str(s).replace(" ", "").strip()

# [안전장치] 데이터 구조 업데이트
for p in st.session_state['projects']:
    keys_defaults = {
        "author_list": [], "reviewer_list": [], "partner_list": [], "issues": [],
        "dev_data": pd.DataFrame(columns=["단원명", "집필자", "집필완료", "검토완료", "피드백완료", "디자인완료", "비고"]),
        "planning_data": pd.DataFrame(), "schedule_data": pd.DataFrame(),
        "book_specs": {
            "format": "", "colors_main": ["1도"], "colors_sol": "1도", 
            "is_ebook": False, "is_answer_view": False, "is_answer_pdf": False
        },
        "report_checklist": pd.DataFrame([
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
        ]),
        "author_standards": pd.DataFrame([{"구분": "기본단가", "지급기준": "쪽당", "원고료_단가": 35000, "검토료_단가": 14000}]),
        "review_standards": pd.DataFrame([
            {"구분": "1차외부검토", "지급기준": "쪽당", "단가": 8000},
            {"구분": "2차외부검토", "지급기준": "쪽당", "단가": 8000},
            {"구분": "3차외부검토", "지급기준": "문항당", "단가": 8000},
            {"구분": "편집검토", "지급기준": "쪽당", "단가": 6000}
        ]),
        "penalties": {},
        "target_date_val": datetime.today(),
        "created_at": datetime.now()
    }
    
    for key, default_val in keys_defaults.items():
        if key not in p:
            p[key] = default_val

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
                if col not in p['dev_data'].columns:
                    p['dev_data'][col] = False
                else:
                    p['dev_data'][col] = p['dev_data'][col].astype(bool)

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
        if role not in existing_std:
            new_std_rows.append({"구분": role, "지급기준": "쪽당", "단가": 0})
    
    if new_std_rows:
        p['review_standards'] = pd.concat([rev_std.drop(columns=['구분_clean']), pd.DataFrame(new_std_rows)], ignore_index=True)
    elif '구분_clean' in rev_std.columns:
        p['review_standards'] = rev_std.drop(columns=['구분_clean'])

    if 'dev_data' in p:
        current_cols = p['dev_data'].columns
        for role in active_roles:
            if role not in current_cols:
                p['dev_data'][role] = "-"

# --- 유틸리티 함수 ---
def get_day_name(date_obj):
    if pd.isnull(date_obj): return ""
    try: return ["(월)", "(화)", "(수)", "(목)", "(금)", "(토)", "(일)"][date_obj.weekday()]
    except: return ""

def validate_email(email): return "@" in str(email)

def get_schedule_date(project, keyword="최종 플루토 OK"):
    df = project.get('schedule_data', pd.DataFrame())
    if df.empty: return None
    mask = df['구분'].astype(str).str.contains(keyword, na=False)
    if mask.any():
        try:
            date_val = df.loc[mask, '종료일'].values[0]
            return pd.to_datetime(date_val)
        except: return None
    return None

def get_notifications():
    notifications = []
    today = datetime.now().date()
    alert_window = 7 
    
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

# --- 데이터 안전장치 함수 ---
def ensure_data_types(df):
    df = df.copy()
    df = df.reset_index(drop=True)
    df["시작일"] = pd.to_datetime(df["시작일"], errors='coerce').dt.date
    df["종료일"] = pd.to_datetime(df["종료일"], errors='coerce').dt.date
    df["소요 일수"] = pd.to_numeric(df["소요 일수"], errors='coerce').fillna(0).astype(int)
    df["선택"] = df["선택"].astype(bool)
    df["독립 일정"] = df["독립 일정"].astype(bool)
    return df

# --- 핵심 로직 (일정) ---
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

def create_initial_schedule(target_date_obj):
    schedule_list = []
    base_date = pd.to_datetime(target_date_obj)
    current_end = base_date
    IMPORTANT_KEYWORDS = ["발주 회의", "집필 (본문 개발)", "1차 외부/교차 검토", "2차 외부/교차 검토", "3차 외부/교차 검토", "가쇄본 제작", "집필자 최종 검토", "내용 OK", "최종 플루토 OK"]

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

# --- 6. 교재(프로젝트) 관리 함수 ---
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
        "report_checklist": pd.DataFrame([
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
        ]),
        "author_standards": pd.DataFrame([{"구분": "기본단가", "지급기준": "쪽당", "원고료_단가": 35000, "검토료_단가": 14000}]),
        "review_standards": pd.DataFrame([
            {"구분": "1차외부검토", "지급기준": "쪽당", "단가": 8000},
            {"구분": "2차외부검토", "지급기준": "쪽당", "단가": 8000},
            {"구분": "3차외부검토", "지급기준": "문항당", "단가": 8000},
            {"구분": "편집검토", "지급기준": "쪽당", "단가": 6000}
        ]),
        "penalties": {},
        "target_date_val": datetime.today(),
        "created_at": datetime.now()
    }
    
    default_target = datetime.today()
    new_p['schedule_data'] = create_initial_schedule(default_target)
    new_p['target_date_val'] = default_target

    st.session_state['projects'].append(new_p)
    st.session_state['current_project_id'] = new_p['id'] 
    st.success(f"[{series}] {title} 교재가 생성되었습니다!")
    st.rerun()

# --- 7. 사이드바 ---
st.sidebar.title("📚 EBS 교재개발 관리")

# [저장 로직]
if st.sidebar.button("☁️ 클라우드 저장 (Google Sheet)", type="primary"):
    with st.spinner("구글 시트에 저장 중..."):
        if save_data_to_sheet(st.session_state['projects']):
            st.sidebar.success("✅ 구글 시트에 안전하게 저장되었습니다!")
        else:
            st.sidebar.error("저장 실패. service_account.json 파일이나 인터넷 연결을 확인하세요.")

st.sidebar.header("📂 교재 선택")

current_p = get_project_by_id(st.session_state['current_project_id'])

# 학교급 정렬
level_order = {"초등": 0, "중학": 1, "고교": 2, "기타": 3}
proj_list_sorted = sorted(
    st.session_state['projects'], 
    key=lambda x: (level_order.get(x['level'], 99), x['year'], x['series'])
)
proj_options = {p['id']: f"[{p['year']}/{p['level']}] {p['series']} - {p['title']}" for p in proj_list_sorted}
proj_options_list = list(proj_options.keys())

current_idx = 0
if current_p and current_p['id'] in proj_options_list:
    current_idx = proj_options_list.index(current_p['id'])

selected_pid = st.sidebar.selectbox(
    "작업할 교재를 선택하세요",
    options=[None] + proj_options_list,
    format_func=lambda x: proj_options[x] if x else "선택 안 함 (새 교재 생성)",
    index=current_idx + 1 if current_p else 0
)

# 교재 변경 시 사이드바 메뉴 초기화
if selected_pid != st.session_state['current_project_id']:
    st.session_state['current_project_id'] = selected_pid
    st.session_state['selected_overview_id'] = selected_pid
    st.session_state['main_menu'] = "교재 등록 및 관리(HOME)" 
    st.rerun()

st.sidebar.markdown("---")

# --- 8. 메뉴 라우팅 ---
menu = st.sidebar.radio(
    "메뉴 이동",
    ["교재 등록 및 관리(HOME)", "1. 교재 기획", "2. 개발 일정", "3. 참여자", "4. 개발 프로세스", "5. 결과보고서 및 정산"],
    key="main_menu"
)

# --- 9. 메인 화면 ---

if menu == "교재 등록 및 관리(HOME)":
    st.title("📊 교재 등록 및 관리")
    
    # 알림
    alerts = get_notifications()
    if alerts:
        with st.expander(f"🔔 마감 임박 알림 ({len(alerts)}건)", expanded=True):
            for a in alerts:
                if a['d_day'] < 0:
                    st.error(f"**{a['project']}** - {a['task']}: 마감일({a['date']})이 지났습니다! (D+{abs(a['d_day'])})")
                elif a['d_day'] == 0:
                    st.error(f"**{a['project']}** - {a['task']}: 오늘 마감입니다!")
                else:
                    st.warning(f"**{a['project']}** - {a['task']}: 마감까지 {a['d_day']}일 남았습니다. ({a['date']})")

    with st.expander("🆕 새로운 교재 생성하기", expanded=not st.session_state['projects']):
        col_new1, col_new2, col_new3, col_new4, col_new5 = st.columns([1, 1, 1, 1.5, 2])
        with col_new1: st.selectbox("개발 연도", [str(y) for y in range(2025, 2031)], key="new_proj_year")
        with col_new2: st.selectbox("학교급", ["초등", "중학", "고교", "기타"], key="new_proj_level")
        with col_new3: st.selectbox("과목", ["국어", "영어", "수학", "사회", "과학", "종합", "기타"], key="new_proj_subject")
        with col_new4: st.text_input("시리즈명", key="new_proj_series")
        with col_new5: st.text_input("교재명", key="new_proj_title")
        if st.button("✨ 교재 생성하기", type="primary"): create_new_project()

    st.markdown("---")
    
    if st.session_state['projects']:
        st.subheader("진행 중인 교재")
        summary_data = []
        for p in proj_list_sorted:
            is_selected = (p['id'] == st.session_state['selected_overview_id'])
            
            target_date = get_schedule_date(p)
            if isinstance(target_date, datetime) or isinstance(target_date, pd.Timestamp):
                 target_date_str = target_date.strftime("%Y-%m-%d")
            else:
                 target_date_val = p.get('target_date_val')
                 if isinstance(target_date_val, datetime):
                     target_date_str = target_date_val.strftime("%Y-%m-%d")
                 else:
                     target_date_str = "-"

            summary_data.append({
                "개요": is_selected, 
                "삭제": False, 
                "연도": p['year'], "학교급": p['level'], "과목": p.get('subject', '-'),
                "시리즈": p['series'], "교재명": p['title'],
                "최종 플루토 OK": target_date_str, "ID": p['id'] 
            })
        
        summary_df = pd.DataFrame(summary_data)
        
        edited_summary_df = st.data_editor(
            summary_df, hide_index=True, key="dashboard_editor",
            column_order=["개요", "연도", "학교급", "과목", "시리즈", "교재명", "최종 플루토 OK", "삭제"],
            column_config={
                "개요": st.column_config.CheckboxColumn("개요", width="small"),
                "삭제": st.column_config.CheckboxColumn("삭제", width="small"),
                "최종 플루토 OK": st.column_config.TextColumn("최종 플루토 OK", width="small"),
            }
        )

        projects_to_delete = edited_summary_df[edited_summary_df['삭제'] == True]
        if not projects_to_delete.empty:
            if st.button("🗑️ 영구 삭제 확인", type="primary"):
                delete_ids = projects_to_delete['ID'].tolist()
                st.session_state['projects'] = [p for p in st.session_state['projects'] if p['id'] not in delete_ids]
                if st.session_state['current_project_id'] in delete_ids:
                    st.session_state['current_project_id'] = None
                st.rerun()

        if not edited_summary_df.equals(summary_df):
            newly_selected_id = None
            for index, row in edited_summary_df.iterrows():
                if not summary_df.iloc[index]['개요'] and row['개요']:
                    newly_selected_id = row['ID']
                    break
            
            if newly_selected_id: 
                st.session_state['current_project_id'] = newly_selected_id
                st.session_state['selected_overview_id'] = newly_selected_id
                st.rerun()
            elif edited_summary_df['개요'].sum() == 0:
                pass 

        if st.session_state['selected_overview_id']:
            target_id = st.session_state['selected_overview_id']
            selected_p = get_project_by_id(target_id)
            if selected_p:
                st.markdown("---")
                st.subheader(f"📌 [{selected_p['series']}] {selected_p['title']} - 상세 개요")
                col_ov1, col_ov2 = st.columns([1, 1])
                with col_ov1:
                    st.info("👥 참여자 현황")
                    raw_authors = [a.get('이름') for a in selected_p.get('author_list', [])]
                    authors = [str(x).strip() for x in raw_authors if x and str(x).lower() not in ['nan', 'none', '']]
                    st.write(f"**✍️ 집필진 ({len(authors)}명):** {', '.join(authors) if authors else '(미등록)'}")
                    
                    raw_reviewers = [r.get('이름') for r in selected_p.get('reviewer_list', [])]
                    reviewers = [str(x).strip() for x in raw_reviewers if x and str(x).lower() not in ['nan', 'none', '']]
                    st.write(f"**🔍 검토진 ({len(reviewers)}명):** {', '.join(reviewers) if reviewers else '(미등록)'}")

                    raw_partners = [p.get('업체명') for p in selected_p.get('partner_list', [])]
                    partners = [str(x).strip() for x in raw_partners if x and str(x).lower() not in ['nan', 'none', '']]
                    st.write(f"**🏢 참여업체:** {', '.join(partners) if partners else '(미등록)'}")

                with col_ov2:
                    st.error("📅 주요 일정")
                    if 'schedule_data' in selected_p and not selected_p['schedule_data'].empty:
                        df_sch = ensure_data_types(selected_p['schedule_data'])
                        major_events = df_sch[df_sch['구분'].str.contains("🔴", na=False)].sort_values("시작일")
                        if not major_events.empty:
                            for _, row in major_events.iterrows():
                                date_str = row['시작일'].strftime("%Y-%m-%d") if pd.notnull(row['시작일']) else "미정"
                                st.write(f"**{date_str}** : {row['구분'].replace('🔴 ','')}")
                        else: st.caption("주요 일정(🔴) 없음")

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
            st.info("교재의 목차와 담당 집필자, 페이지 수 등을 관리합니다.")
            
            col_sync, _ = st.columns([1, 4])
            with col_sync:
                if st.button("🔄 데이터 연동 (Sync)", type="primary"):
                    plan_df = current_p.get('planning_data', pd.DataFrame())
                    if not plan_df.empty:
                        if '집필자' in plan_df.columns:
                            existing = [a['이름'] for a in current_p.get('author_list', [])]
                            for auth in plan_df['집필자'].unique():
                                if pd.notnull(auth) and str(auth).strip() not in ['-', ''] and auth not in existing:
                                    current_p['author_list'].append({"이름": auth, "역할": "공동집필"})
                        
                        if '대단원' in plan_df.columns:
                            dev_df = current_p.get('dev_data', pd.DataFrame())
                            existing_units = dev_df['단원명'].tolist() if '단원명' in dev_df.columns else []
                            new_rows = []
                            for _, row in plan_df.iterrows():
                                name = f"[{row.get('분권','')}] {row.get('대단원','')} > {row.get('중단원','')}"
                                if name not in existing_units:
                                    author_name = row.get('집필자', '') 
                                    new_rows.append({"단원명": name, "집필자": author_name if pd.notnull(author_name) else ""})
                            if new_rows:
                                new_df = pd.DataFrame(new_rows)
                                for col in dev_df.columns:
                                    if col not in new_df.columns: new_df[col] = "" 
                                current_p['dev_data'] = pd.concat([dev_df, new_df], ignore_index=True)
                                st.toast("✅ 연동 완료")
            
            uploaded_file = st.file_uploader("배열표 엑셀/CSV 파일 업로드", type=["xlsx", "xls", "csv"])
            if uploaded_file:
                try:
                    if uploaded_file.name.endswith('.csv'): df_upload = pd.read_csv(uploaded_file)
                    else: df_upload = pd.read_excel(uploaded_file)
                    
                    if '분권' in df_upload.columns: df_upload['분권'] = df_upload['분권'].fillna(method='ffill')
                    if '대단원' in df_upload.columns: df_upload['대단원'] = df_upload['대단원'].fillna(method='ffill')
                    if '구분' in df_upload.columns: df_upload['구분'] = df_upload['구분'].fillna("") 

                    update_current_project_data('planning_data', df_upload)
                    st.success("파일 업로드 완료!")
                except Exception as e: st.error(f"파일 읽기 실패: {e}")

            plan_df = current_p.get('planning_data', pd.DataFrame())
            if not plan_df.empty:
                edited_plan = st.data_editor(plan_df, num_rows="dynamic", key="planning_editor")
                if not edited_plan.equals(plan_df):
                    update_current_project_data('planning_data', edited_plan)
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
                
                if st.button("➕ 본문 도수 추가"):
                    specs["colors_main"].append("1도")
                    update_current_project_data('book_specs', specs)
                    st.rerun()

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
            st.markdown("##### 🛠️ 일정 관리 도구")
            col_date, col_calc, col_reset, col_ics = st.columns([2, 1.5, 1.5, 2])
            
            with col_date:
                schedule_date = get_schedule_date(current_p)
                default_date = schedule_date if schedule_date else current_p.get('target_date_val', datetime.today())
                target_date = st.date_input("기준일 (최종 플루토 OK)", default_date)
                if target_date != default_date:
                     update_current_project_data('target_date_val', target_date)
            
            with col_calc:
                st.markdown(" ") 
                if st.button("⚡ 기준일로 전체 자동 계산", type="primary", help="기준일을 바탕으로 모든 일정을 자동으로 역산합니다."):
                     final_df = recalculate_dates(current_p['schedule_data'], target_date)
                     update_current_project_data('schedule_data', final_df)
                     st.rerun()
            
            with col_reset:
                st.markdown(" ") 
                if st.button("🔄 초기화 (기본값 복구)", help="주의: 모든 일정이 초기화됩니다."):
                     schedule_df = create_initial_schedule(target_date)
                     update_current_project_data('schedule_data', schedule_df)
                     st.rerun()
            
            with col_ics:
                st.markdown(" ") 
                df_ics = current_p.get('schedule_data', pd.DataFrame())
                if not df_ics.empty:
                    ics_data = create_ics_file(ensure_data_types(df_ics), current_p['title'])
                    st.download_button(
                        label="⬇️ ICS 캘린더 파일 다운로드",
                        data=ics_data,
                        file_name=f"{current_p['series']}_{current_p['title']}_Schedule.ics",
                        mime="text/calendar",
                        type="secondary"
                    )

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
             st.rerun()

    # ==========================================
    # [3. 참여자] 
    # ==========================================
    elif menu == "3. 참여자":
        st.title("👥 참여자 관리")
        tab_auth, tab_rev, tab_partner = st.tabs(["📝 집필진", "🔍 검토진", "🏢 참여업체"])

        with tab_auth:
            st.info("💡 집필진의 이름은 '교재 기획 > 배열표'에 입력된 이름과 정확히 일치해야 단원이 자동 매칭됩니다.")
            plan_df = current_p.get('planning_data', pd.DataFrame())
            
            author_map = {}
            if not plan_df.empty and '집필자' in plan_df.columns:
                 if '대단원' not in plan_df.columns: plan_df['대단원'] = ""
                 plan_df['Full단원'] = plan_df['대단원']
                 author_map = plan_df.groupby('집필자')['Full단원'].apply(list).to_dict()

            with st.form("author_form", clear_on_submit=True, border=True):
                st.markdown("##### ➕ 집필진 수동 등록")
                col1, col2, col3, col4, col5 = st.columns([1, 1, 1.5, 1.5, 1.2])
                with col1: name = st.text_input("이름 *", key="auth_name")
                with col2: school = st.selectbox("학교급 *", ["초등", "중학", "고교"], key="auth_school")
                with col3: affil = st.text_input("소속 *", key="auth_affil")
                with col4: subj = st.selectbox("담당 과목 *", ["물리학", "화학", "생명과학", "지구과학", "공통", "기타"], key="auth_subj")
                with col5: role = st.radio("역할 *", ["대표집필", "공동집필"], horizontal=True, key="auth_role")
                
                if name and name in author_map:
                    st.success(f"✅ 배열표상 배정된 단원: {', '.join(author_map[name][:3])}...")

                col_b1, col_b2 = st.columns(2)
                with col_b1: phone = st.text_input("휴대전화 *", key="auth_phone")
                with col_b2: email = st.text_input("이메일 *", key="auth_email")
                
                with st.expander("배송 및 정산 정보 (선택)"):
                    col_c1, col_c2 = st.columns([1, 4])
                    with col_c1: zipcode = st.text_input("우편번호", key="auth_zip")
                    with col_c2: addr = st.text_input("도로명/지번 주소", key="auth_addr")
                    addr_detail = st.text_input("상세 주소", key="auth_detail")
                    col_d1, col_d2, col_d3 = st.columns([1, 2, 1])
                    with col_d1: bank = st.text_input("은행명", key="auth_bank")
                    with col_d2: account = st.text_input("계좌번호", key="auth_account")
                    with col_d3: rid = st.text_input("주민번호 앞 6자리", max_chars=6, key="auth_rid")
                
                if st.form_submit_button("집필진 등록", type="primary"):
                    if not name: st.error("이름은 필수입니다.")
                    else:
                        new_data = {"이름": name, "학교급": school, "소속": affil, "과목": subj, "역할": role, "연락처": phone, "이메일": email, "우편번호": zipcode, "주소": addr, "상세주소": addr_detail, "은행명": bank, "계좌번호": account, "주민번호(앞)": rid}
                        current_p['author_list'].append(new_data)
                        st.success(f"집필진 '{name}' 등록 완료!"); st.rerun()
            
            if current_p['author_list']:
                auth_df = pd.DataFrame(current_p['author_list'])
                cols = ["이름", "학교급", "소속", "과목", "역할", "연락처", "이메일", "우편번호", "주소", "상세주소", "은행명", "계좌번호", "주민번호(앞)"]
                for c in cols:
                    if c not in auth_df.columns: auth_df[c] = ""
                
                st.data_editor(
                    auth_df, 
                    hide_index=True, 
                    key="auth_list_editor",
                    column_order=cols,
                    column_config={"이메일": st.column_config.TextColumn("이메일")} 
                )
            else:
                st.info("등록된 집필진이 없습니다.")

        with tab_rev:
            st.info("검토진 정보를 관리합니다.")
            dev_df = current_p.get('dev_data', pd.DataFrame())
            existing_authors = [a['이름'] for a in current_p.get('author_list', []) if a.get('이름')]
            
            plan_df = current_p.get('planning_data', pd.DataFrame())
            if not plan_df.empty and '대단원' in plan_df.columns:
                 existing_units = [str(x).strip() for x in plan_df['대단원'].dropna().unique()]
            else:
                 existing_units = []

            st.write("###### 🔗 매칭 정보 설정 (자동 배정 기준)")
            match_mode = st.radio("매칭 기준 선택", ["집필자 기준 (추천)", "단원 기준"], horizontal=True, key="match_mode_radio")

            with st.form("rev_form", clear_on_submit=True, border=True):
                st.markdown("##### ➕ 검토진 수동 등록")
                col1, col2, col3, col4, col5 = st.columns([1, 1, 1.5, 1.5, 1.2])
                with col1: name = st.text_input("이름 *", key="r_name")
                with col2: school = st.selectbox("학교급 *", ["초등", "중학", "고교"], key="r_school")
                with col3: affil = st.text_input("소속 *", key="r_affil")
                with col4: subj = st.selectbox("담당 과목 *", ["물리학", "화학", "생명과학", "지구과학", "공통", "기타"], key="r_subj")
                with col5: 
                    role_options = ["1차 외부검토", "2차 외부검토", "3차 외부검토", "편집검토", "감수", "직접 입력"]
                    role_sel = st.selectbox("검토 차수 *", role_options, key="r_role_sel")
                
                role_input = ""
                if role_sel == "직접 입력":
                    role_input = st.text_input("검토 차수 직접 입력", key="r_role_input")
                else:
                    role_input = role_sel

                col_b1, col_b2 = st.columns(2)
                with col_b1: phone = st.text_input("휴대전화 *", key="r_phone")
                with col_b2: email = st.text_input("이메일 *", key="r_email")
                
                match_val = ""
                if match_mode == "집필자 기준 (추천)":
                    if existing_authors:
                        match_val = st.selectbox("담당 집필자 선택", ["선택 안 함"] + existing_authors, key="select_match_author")
                else:
                    if existing_units:
                        selected_units = st.multiselect("검토 대단원 선택 (복수 가능)", existing_units, key="select_match_unit")
                        if selected_units: match_val = ", ".join(selected_units)

                with st.expander("배송 및 정산 정보 (선택)"):
                    col_c1, col_c2 = st.columns([1, 4])
                    with col_c1: zipcode = st.text_input("우편번호", key="r_zip")
                    with col_c2: addr = st.text_input("도로명/지번 주소", key="r_addr")
                    addr_detail = st.text_input("상세 주소", key="r_addr_detail")
                    col_d1, col_d2, col_d3 = st.columns([1, 2, 1])
                    with col_d1: bank = st.text_input("은행명", key="r_bank")
                    with col_d2: account = st.text_input("계좌번호", key="r_account")
                    with col_d3: rid = st.text_input("주민번호 앞 6자리", max_chars=6, key="r_rid")

                if st.form_submit_button("검토진 등록", type="primary"):
                    final_role = role_input.strip()
                    if not name or not final_role: st.error("이름과 검토 차수는 필수입니다.")
                    else:
                        role_clean = normalize_string(final_role)
                        new_data = {"이름": name, "검토차수": role_clean, "매칭정보": match_val, "소속": affil, "연락처": phone, "이메일": email, "우편번호": zipcode, "주소": addr, "상세주소": addr_detail, "은행명": bank, "계좌번호": account, "주민번호(앞)": rid}
                        current_p['reviewer_list'].append(new_data)
                        
                        rev_std = current_p['review_standards']
                        if role_clean not in rev_std['구분'].apply(normalize_string).values:
                            new_std = pd.DataFrame([{"구분": role_clean, "지급기준": "쪽당", "단가": 0}])
                            current_p['review_standards'] = pd.concat([rev_std, new_std], ignore_index=True)
                        
                        dev_df = current_p['dev_data']
                        if role_clean not in dev_df.columns:
                            dev_df[role_clean] = "-"
                            current_p['dev_data'] = dev_df
                        
                        st.success("등록 완료")
                        st.rerun()
            
            if current_p['reviewer_list']:
                rev_df = pd.DataFrame(current_p['reviewer_list'])
                cols = ["이름", "학교급", "소속", "과목", "검토차수", "매칭정보", "연락처", "이메일", "우편번호", "주소", "상세주소", "은행명", "계좌번호", "주민번호(앞)"]
                for c in cols:
                    if c not in rev_df.columns: rev_df[c] = ""
                
                st.data_editor(
                    rev_df, 
                    hide_index=True, 
                    key="reviewer_list_editor",
                    column_order=cols,
                    column_config={"이메일": st.column_config.TextColumn("이메일")} 
                )
            else:
                st.info("등록된 검토진이 없습니다.")

        with tab_partner:
            st.info("편집, 인쇄, 디자인 등 협력 업체 정보를 관리합니다.")
            with st.form("partner_form", clear_on_submit=True, border=True):
                st.markdown("##### ➕ 업체 정보 입력")
                col_p1, col_p2 = st.columns(2)
                with col_p1: p_name = st.text_input("업체명 *", key="p_name")
                with col_p2: 
                    p_types = st.multiselect("참여 분야 (선택)", ["편집", "표지", "인쇄", "사진", "가쇄본"], key="p_type_select")
                    p_type_direct = st.text_input("참여 분야 (직접 입력)", key="p_type_direct")
                col_p3, col_p4, col_p5 = st.columns(3)
                with col_p3: p_person = st.text_input("담당자명", key="p_person")
                with col_p4: p_contact = st.text_input("연락처", key="p_contact")
                with col_p5: p_email = st.text_input("이메일", key="p_email")
                p_note = st.text_area("비고 (메모)", key="p_note")
                
                if st.form_submit_button("업체 등록", type="primary"):
                    if not p_name: st.error("업체명 필수")
                    else:
                        final_roles = ", ".join(p_types + ([p_type_direct] if p_type_direct else []))
                        new_data = {"업체명": p_name, "분야": final_roles, "담당자": p_person, "연락처": p_contact, "이메일": p_email, "비고": p_note}
                        current_p['partner_list'].append(new_data)
                        st.success("등록 완료")
                        st.rerun()
            
            if current_p['partner_list']:
                part_df = pd.DataFrame(current_p['partner_list'])
                cols = ["업체명", "분야", "담당자", "연락처", "이메일", "비고"]
                for c in cols:
                    if c not in part_df.columns: part_df[c] = ""
                
                st.data_editor(
                    part_df, 
                    hide_index=True, 
                    key="partner_list_editor",
                    column_order=cols,
                    column_config={"이메일": st.column_config.TextColumn("이메일")} 
                )
            else:
                st.info("등록된 협력 업체가 없습니다.")

    # ==========================================
    # [4. 개발 프로세스] 
    # ==========================================
    elif menu == "4. 개발 프로세스":
        st.title("⚙️ 개발 프로세스 관리")
        tab_status, tab_detail, tab_progress = st.tabs(["참여자 배정", "상세 진행 관리", "진행 상황"])
        
        with tab_status:
            col_title, col_btn = st.columns([4, 1])
            with col_title:
                st.markdown("##### 📝 단원별 집필/검토자 배정 매트릭스")
            with col_btn:
                if st.button("🔄 검토자 자동 배정", type="primary"):
                    dev_df = current_p['dev_data']
                    cnt = 0
                    for r in current_p['reviewer_list']:
                        match_targets = [t.strip() for t in str(r.get('매칭정보','')).split(',')]
                        role_col = normalize_string(r.get('검토차수'))
                        
                        if role_col in dev_df.columns:
                            for idx, row in dev_df.iterrows():
                                if any(t in str(row['단원명']) for t in match_targets) or \
                                   any(t == str(row['집필자']) for t in match_targets):
                                    if dev_df.at[idx, role_col] in ["-", "", None]:
                                        dev_df.at[idx, role_col] = r['이름']
                                        cnt += 1
                    current_p['dev_data'] = dev_df
                    st.success(f"매칭 정보를 기반으로 {cnt}건의 검토자 배정 완료!")
                    st.rerun()

            dev_df = current_p['dev_data']
            base_cols = ["단원명", "집필자"]
            review_cols = [c for c in dev_df.columns if ("검토" in c or "감수" in c) and c not in ["검토상태", "검토자", "검토료_단가"] and c not in ["집필완료", "검토완료", "피드백완료", "디자인완료"]]
            final_cols = base_cols + review_cols
            valid_cols = [c for c in final_cols if c in dev_df.columns]
            
            edited = st.data_editor(dev_df[valid_cols], hide_index=True, key="dev_process_matrix_editor")
            if not edited.equals(dev_df[valid_cols]):
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
                    status = "✅ 완료" if row['종료일'] < today else ("🏃 진행중" if (pd.notnull(row['시작일']) and pd.notnull(row['종료일']) and row['시작일'] <= today <= row['종료일']) else "⚪ 대기")
                    
                    if row['구분'].startswith("🔴"):
                         st.error(f"**{status}** | **{row['구분'].replace('🔴 ','')}** ({row['시작일']} ~ {row['종료일']})")
                    else:
                         st.write(f"**{status}** | {row['구분']} ({row['시작일']} ~ {row['종료일']})")
            else:
                st.info("등록된 일정이 없습니다. '개발 일정' 메뉴에서 일정을 생성해 주세요.")

    # ==========================================
    # [5. 결과보고서 및 정산] 
    # ==========================================
    elif menu == "5. 결과보고서 및 정산":
        st.title("📑 결과보고서 및 정산")
        tab_report, tab_settle = st.tabs(["결과보고서", "정산"])
        
        with tab_report:
            st.markdown("##### 📎 필수 서류 구비 체크리스트")
            checklist_df = current_p.get('report_checklist', pd.DataFrame())
            edited_checklist = st.data_editor(
                checklist_df,
                column_config={
                    "구분": st.column_config.TextColumn("구분", disabled=True),
                    "내용": st.column_config.TextColumn("내용", disabled=True),
                    "완료": st.column_config.CheckboxColumn("완료 확인", width="small")
                },
                hide_index=True,
                num_rows="fixed",
                key="report_checklist_editor"
            )
            if not edited_checklist.equals(checklist_df):
                update_current_project_data('report_checklist', edited_checklist)
                st.rerun()
                
            total_items = len(checklist_df)
            done_items = checklist_df['완료'].sum()
            progress_ratio = done_items/total_items if total_items > 0 else 0
            
            st.metric("서류 구비 현황", f"{int(progress_ratio*100)}%", delta=f"{done_items}/{total_items}건 완료", delta_color="off")
            st.progress(progress_ratio)

        with tab_settle:
            st.subheader("1. 기준 단가 설정")
            col_set1, col_set2 = st.columns(2)
            
            with col_set1:
                st.markdown("###### ✍️ 집필료 기준")
                auth_std_df = current_p['author_standards']
                edited_auth_std = st.data_editor(
                    auth_std_df, num_rows="dynamic", hide_index=True, key="auth_std_editor",
                    column_config={
                        "구분": st.column_config.TextColumn("구분"),
                        "지급기준": st.column_config.SelectboxColumn("지급기준", options=["쪽당", "문항당"], width="small"),
                        "원고료_단가": st.column_config.NumberColumn("원고료(원)", format="%d원"),
                        "검토료_단가": st.column_config.NumberColumn("검토료(원)", format="%d원"),
                    }
                )
                if not edited_auth_std.equals(auth_std_df):
                    update_current_project_data('author_standards', edited_auth_std)
                    st.rerun()

            with col_set2:
                st.markdown("###### 🔍 검토료 기준")
                rev_std_df = current_p.get('review_standards', pd.DataFrame())
                edited_rev_std = st.data_editor(
                    rev_std_df, num_rows="dynamic", hide_index=True, key="rev_std_editor",
                    column_config={
                        "구분": st.column_config.TextColumn("구분 (예: 1차 외부검토)"),
                        "지급기준": st.column_config.SelectboxColumn("지급기준", options=["쪽당", "문항당"], width="small", default="쪽당"),
                        "단가": st.column_config.NumberColumn("단가 (원)", format="%d원"),
                    }
                )
                if not edited_rev_std.equals(rev_std_df):
                    update_current_project_data('review_standards', edited_rev_std)
                    st.rerun()

            st.markdown("---")
            
            st.subheader("2. 정산 내역서")
            plan_df = current_p.get('planning_data', pd.DataFrame())
            dev_df = current_p.get('dev_data', pd.DataFrame())

            st.markdown("#### ✍️ 집필료")
            if not plan_df.empty and '집필자' in plan_df.columns:
                if '쪽수' not in plan_df.columns: plan_df['쪽수'] = 0
                if '문항수' not in plan_df.columns: plan_df['문항수'] = 0 
                plan_df['쪽수'] = pd.to_numeric(plan_df['쪽수'], errors='coerce').fillna(0)
                plan_df['문항수'] = pd.to_numeric(plan_df['문항수'], errors='coerce').fillna(0)
                
                author_stats = plan_df.groupby('집필자')[['쪽수', '문항수']].sum().reset_index()
                author_stats = author_stats[author_stats['집필자'] != '-']
                
                std_row = current_p['author_standards'].iloc[0] if not current_p['author_standards'].empty else {}
                basis = std_row.get('지급기준', '쪽당')
                price_write = std_row.get('원고료_단가', 0)
                price_review = std_row.get('검토료_단가', 0)
                
                if basis == '쪽당': author_stats['적용수량'] = author_stats['쪽수']
                else: author_stats['적용수량'] = author_stats['문항수']
                    
                author_stats['원고료'] = author_stats['적용수량'] * price_write
                author_stats['검토료'] = author_stats['적용수량'] * price_review
                author_stats['총지급액'] = author_stats['원고료'] + author_stats['검토료']
                author_stats['1차지급(70%)'] = author_stats['총지급액'] * 0.7
                
                current_penalties = current_p.get('penalties', {})
                author_stats['패널티'] = author_stats['집필자'].apply(lambda x: current_penalties.get(x, 0))
                author_stats['2차지급(30%)'] = (author_stats['총지급액'] * 0.3) - author_stats['패널티']
                
                display_auth = author_stats[['집필자', '적용수량', '원고료', '검토료', '총지급액', '1차지급(70%)', '패널티', '2차지급(30%)']].copy()
                
                edited_auth = st.data_editor(
                    display_auth, 
                    column_config={
                        "집필자": st.column_config.TextColumn("집필자", disabled=True),
                        "총지급액": st.column_config.NumberColumn(format="%d원", disabled=True),
                        "원고료": st.column_config.NumberColumn(format="%d원", disabled=True),
                        "검토료": st.column_config.NumberColumn(format="%d원", disabled=True),
                        "1차지급(70%)": st.column_config.NumberColumn(format="%d원", disabled=True),
                        "패널티": st.column_config.NumberColumn(format="%d원"), 
                        "2차지급(30%)": st.column_config.NumberColumn(format="%d원", disabled=True),
                    },
                    hide_index=True, key="author_settlement_editor"
                )
                if not edited_auth.equals(display_auth):
                    for index, row in edited_auth.iterrows():
                        current_p['penalties'][row['집필자']] = row['패널티']
                    update_current_project_data('penalties', current_p['penalties'])
                    st.rerun()
                st.metric("집필료 총계", f"**{int(display_auth['총지급액'].sum()):,}**원")
            else:
                st.warning("집필자 데이터가 없습니다.")

            st.markdown("---")

            st.markdown("#### 🔍 검토료")
            if not dev_df.empty:
                reviewer_calc_list = []
                std_map = {}
                for _, row in rev_std_df.iterrows():
                    clean_name = normalize_string(row['구분'])
                    std_map[clean_name] = {"name": row['구분'], "price": row['단가']}

                for _, row in dev_df.iterrows():
                    unit_name = str(row.get('단원명', ''))
                    matched_plan = plan_df[plan_df.apply(lambda x: str(x.get('대단원')) in unit_name, axis=1)]
                    
                    if '쪽수' in plan_df.columns:
                        page_count = matched_plan['쪽수'].sum() if not matched_plan.empty else 0
                    else:
                        page_count = 0

                    for col in dev_df.columns:
                        col_clean = normalize_string(col)
                        if col_clean in std_map: 
                            reviewer_name = row[col]
                            if reviewer_name and str(reviewer_name).strip() not in ['-', '', 'nan', 'None']:
                                price = std_map[col_clean]['price']
                                std_name = std_map[col_clean]['name']
                                reviewer_calc_list.append({
                                    "검토자": reviewer_name,
                                    "구분": std_name,
                                    "수량": page_count,
                                    "단가": price,
                                    "금액": page_count * price
                                })
                            
                if reviewer_calc_list:
                    rev_calc_df = pd.DataFrame(reviewer_calc_list)
                    rev_summary = rev_calc_df.groupby(['검토자', '구분'])['금액'].sum().reset_index()
                    rev_summary['1차지급(80%)'] = rev_summary['금액'] * 0.8
                    
                    rev_summary['UniqueKey'] = rev_summary['검토자'] + "_" + rev_summary['구분']
                    rev_summary['패널티'] = rev_summary['UniqueKey'].apply(lambda x: current_penalties.get(x, 0))
                    rev_summary['2차지급(20%)'] = (rev_summary['금액'] * 0.2) - rev_summary['패널티']
                    
                    sort_order = {"1차": 1, "2차": 2, "3차": 3, "편집": 4, "감수": 5, "기타": 99}
                    def get_sort_key(val):
                        for k, v in sort_order.items():
                            if k in str(val): return v
                        return 99
                    rev_summary['SortKey'] = rev_summary['구분'].apply(get_sort_key)
                    
                    subtotals = rev_summary.groupby('구분')[['금액', '1차지급(80%)', '패널티', '2차지급(20%)']].sum().reset_index()
                    subtotals['검토자'] = '🟦 [소계]'
                    subtotals['UniqueKey'] = subtotals['구분'] + "_total"
                    subtotals['SortKey'] = subtotals['구분'].apply(get_sort_key)
                    
                    final_df = pd.concat([rev_summary, subtotals], ignore_index=True)
                    final_df['IsTotal'] = final_df['검토자'].apply(lambda x: 1 if '소계' in x else 0)
                    final_df = final_df.sort_values(by=['SortKey', 'IsTotal', '검토자']).drop(columns=['SortKey', 'IsTotal'])

                    display_rev = final_df[['구분', '검토자', '금액', '1차지급(80%)', '패널티', '2차지급(20%)', 'UniqueKey']].copy()
                    
                    edited_rev = st.data_editor(
                        display_rev,
                        column_config={
                            "구분": st.column_config.TextColumn("구분", disabled=True),
                            "검토자": st.column_config.TextColumn("검토자", disabled=True),
                            "금액": st.column_config.NumberColumn("총지급액", format="%d원", disabled=True),
                            "1차지급(80%)": st.column_config.NumberColumn(format="%d원", disabled=True),
                            "패널티": st.column_config.NumberColumn(format="%d원"), 
                            "2차지급(20%)": st.column_config.NumberColumn(format="%d원", disabled=True),
                            "UniqueKey": None 
                        },
                        hide_index=True, key="reviewer_settlement_editor"
                    )
                    
                    if not edited_rev.equals(display_rev):
                        for index, row in edited_rev.iterrows():
                            if "소계" not in row['검토자']:
                                u_key = row['UniqueKey']
                                penalty = row['패널티']
                                current_p['penalties'][u_key] = penalty
                        update_current_project_data('penalties', current_p['penalties'])
                        st.rerun()
                    st.metric("검토료 총계", f"**{int(rev_summary['금액'].sum()):,}**원")
                else:
                    st.info("검토자가 배정된 내역이 없거나, 단원 매칭이 되지 않았습니다. (쪽수 확인 필요)")
            else:
                st.warning("개발 프로세스 데이터가 없습니다.")