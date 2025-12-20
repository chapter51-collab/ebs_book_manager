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
import streamlit.components.v1 as components 
from PIL import Image

# [Library Check]
try:
    from streamlit_drawable_canvas import st_canvas
except ImportError:
    st.error("라이브러리 설치 필요: pip install streamlit-drawable-canvas")
    st.stop()

# --- 1. 페이지 설정 ---
st.set_page_config(page_title="EBS 교재개발 관리 프로그램", page_icon="📚", layout="wide")

# [Custom CSS]
st.markdown("""
<style>
    button[data-baseweb="tab"] { font-size: 16px; font-weight: 500; color: #555; }
    button[data-baseweb="tab"][aria-selected="true"] {
        color: #E53935 !important; font-weight: 800 !important;
        background-color: rgba(229, 57, 53, 0.05); border-bottom: 3px solid #E53935 !important;
    }
    .metric-box {
        border: 1px solid #e0e0e0; border-radius: 8px; padding: 15px;
        text-align: center; background-color: #ffffff; box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    .metric-label { font-size: 1.4rem; font-weight: 800; color: #333; margin-bottom: 5px; }
    .metric-value { font-size: 1rem; font-weight: 500; color: #666; }
</style>
""", unsafe_allow_html=True)

if 'view_mode' not in st.session_state: st.session_state['view_mode'] = 'admin'
if 'active_token' not in st.session_state: st.session_state['active_token'] = None

# --- 2. 헬퍼 함수 ---
def normalize_string(s): return str(s).replace(" ", "").strip()
def clean_korean_date(date_str):
    if pd.isna(date_str): return None
    return re.sub(r'\s*\(.*?\)', '', str(date_str)).strip()

def safe_to_numeric(series):
    return pd.to_numeric(series.astype(str).str.replace(',', '').str.replace('원', ''), errors='coerce').fillna(0)

def get_sort_rank(content_str):
    s = normalize_string(str(content_str))
    if "1차" in s: return 1
    elif "2차" in s: return 2
    elif "3차" in s: return 3
    elif "편집" in s: return 4
    elif "감수" in s: return 5
    return 99 

def image_to_base64(image_file):
    if image_file is None: return None
    try:
        if isinstance(image_file, bytes): return base64.b64encode(image_file).decode()
        return base64.b64encode(image_file.getvalue()).decode()
    except: return None

def format_person_label(info):
    """동명이인 식별용 라벨 생성 (오류 방지)"""
    try:
        name = str(info.get('이름', '미상'))
        affil = str(info.get('소속', '')) if pd.notnull(info.get('소속')) else ""
        phone = str(info.get('연락처', '')) if pd.notnull(info.get('연락처')) else ""
        suffix = phone[-4:] if len(phone) >= 4 else ""
        desc = f"{affil}, {suffix}" if affil or suffix else ""
        return f"{name} ({desc})" if desc else name
    except: return str(info.get('이름', '오류'))

# --- 3. 데이터 로드/저장 ---
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
SHEET_NAME = "EBS_Book_DB" 

def get_db_connection():
    try:
        if os.path.exists("service_account.json"):
            creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", SCOPE)
        elif "gcp_service_account" in st.secrets:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(dict(st.secrets["gcp_service_account"]), SCOPE)
        else: return None
        return gspread.authorize(creds).open(SHEET_NAME).sheet1
    except: return None

def load_data_from_sheet():
    sheet = get_db_connection()
    if sheet:
        try:
            val = sheet.col_values(1)
            if val: return pickle.loads(base64.b64decode("".join(val)))
        except: pass
    return None

def save_data_to_sheet(data_pkg):
    sheet = get_db_connection()
    if sheet:
        try:
            b64 = base64.b64encode(pickle.dumps(data_pkg)).decode('utf-8')
            chunks = [b64[i:i+45000] for i in range(0, len(b64), 45000)]
            sheet.clear()
            sheet.update(range_name=f'A1:A{len(chunks)}', values=[[c] for c in chunks])
            return True
        except: return False
    return False

# --- 4. 데이터 마이그레이션 (인물 DB 분리) ---
def migrate_and_initialize():
    data = load_data_from_sheet()
    if not data and os.path.exists("book_project_data.pkl"):
         try:
            with open("book_project_data.pkl", 'rb') as f: data = pickle.load(f)
         except: pass

    if not data:
        st.session_state['projects'] = []
        st.session_state['people'] = {}
        return

    if isinstance(data, list): # 구 버전 -> 신 버전 변환
        projects = data
        people_db = {}
        for p in projects:
            # 집필진 추출
            new_links = []
            if 'author_list' in p:
                for auth in p['author_list']:
                    if isinstance(auth, dict) and 'person_id' not in auth:
                        pid = str(uuid.uuid4())[:8]
                        found = False
                        for eid, info in people_db.items():
                            if info['이름'] == auth.get('이름'):
                                if str(info.get('연락처','')) == str(auth.get('연락처','')):
                                    pid = eid; found = True; break
                        if not found: people_db[pid] = auth
                        new_links.append({"person_id": pid, "역할": auth.get('역할', '공동집필')})
                    elif isinstance(auth, dict): new_links.append(auth)
            p['author_links'] = new_links
            
            # 검토진 추출
            new_rev_links = []
            if 'reviewer_list' in p:
                for rev in p['reviewer_list']:
                    if isinstance(rev, dict) and 'person_id' not in rev:
                        pid = str(uuid.uuid4())[:8]
                        found = False
                        for eid, info in people_db.items():
                            if info['이름'] == rev.get('이름'):
                                if str(info.get('연락처','')) == str(rev.get('연락처','')):
                                    pid = eid; found = True; break
                        if not found: people_db[pid] = rev
                        new_rev_links.append({"person_id": pid, "검토차수": rev.get('검토차수'), "매칭정보": rev.get('매칭정보')})
                    elif isinstance(rev, dict): new_rev_links.append(rev)
            p['reviewer_links'] = new_rev_links

            if 'partner_list' not in p: p['partner_list'] = []
            if 'dev_data' not in p: p['dev_data'] = pd.DataFrame()
            
        st.session_state['projects'] = projects
        st.session_state['people'] = people_db
        save_data_to_sheet({'projects': projects, 'people': people_db})
        
    elif isinstance(data, dict):
        st.session_state['projects'] = data.get('projects', [])
        st.session_state['people'] = data.get('people', {})

if 'projects' not in st.session_state: migrate_and_initialize()

def save_current_state():
    return save_data_to_sheet({'projects': st.session_state['projects'], 'people': st.session_state['people']})

def get_project_by_id(pid):
    for p in st.session_state['projects']:
        if p['id'] == pid: return p
    return None

# 데이터 해시 확인
def get_data_hash(data): return hashlib.md5(pickle.dumps(data)).hexdigest()
if 'last_saved_hash' not in st.session_state:
    st.session_state['last_saved_hash'] = get_data_hash({'projects':st.session_state['projects'], 'people':st.session_state['people']})

# --- 사이드바 ---
st.sidebar.title("📚 EBS 교재개발 관리")
curr_hash = get_data_hash({'projects':st.session_state['projects'], 'people':st.session_state['people']})
if curr_hash != st.session_state['last_saved_hash']:
    st.sidebar.warning("⚠️ 저장되지 않은 변경사항이 있습니다!")
    if st.sidebar.button("💾 변경 사항 저장"):
        if save_current_state():
            st.session_state['last_saved_hash'] = curr_hash
            st.sidebar.success("저장되었습니다.")
            st.rerun()
else:
    st.sidebar.button("✅ 최신 상태입니다", disabled=True)

if st.sidebar.button("🔄 서버 데이터 다시 불러오기"):
    migrate_and_initialize(); st.rerun()

current_p = get_project_by_id(st.session_state.get('current_project_id'))
st.sidebar.markdown("---")
menu = st.sidebar.radio("메뉴", ["교재 등록 및 개요(HOME)", "1. 교재 관리", "2. 참여자 관리", "3. 집필 및 검토 관리", "4. 개발 후 관리(정산 및 결과 보고)", "5. 약정서 관리"])
if current_p:
    st.sidebar.info(f"📂 **{current_p['title']}**\n\n({current_p['series']})")

# --- 유틸리티 함수들 ---
def create_initial_schedule(target):
    lst = []
    curr = pd.to_datetime(target)
    def add(name, d, indep=False, note=""):
        nonlocal curr
        nm = f"🔴 {name}" if any(k in name for k in ["발주", "집필", "검토", "가쇄본", "OK", "플루토"]) else name
        s = curr - timedelta(days=d-1)
        lst.append({"선택":False, "독립 일정":indep, "구분":nm, "소요 일수":d, "시작일":s.date(), "종료일":curr.date(), "비고":note})
        if not indep: curr = s - timedelta(days=1)
    add("최종 플루토 OK", 2, note="★ 기준"); add("내용 OK", 3); add("인쇄협의체 회의", 1, True)
    add("최종 검토 반영", 7); add("집필자 최종 검토", 1); add("편집 검토", 7); add("가쇄본 제작", 3)
    for i in range(3,0,-1): add(f"{i}차 조판 수정", 7); add(f"{i}차 집필자 반영", 7); add(f"{i}차 외부/교차 검토", 7)
    add("1차 조판 및 편집", 40); add("집필 (본문 개발)", 30); add("발주 회의 및 계약", 1)
    lst.reverse()
    base = pd.to_datetime(target)
    lst.append({"선택":False, "독립 일정":False, "구분":"최종 PDF 수령", "소요 일수":3, "시작일":(base+timedelta(1)).date(), "종료일":(base+timedelta(3)).date(), "비고":""})
    lst.append({"선택":False, "독립 일정":False, "구분":"💰 정산", "소요 일수":0, "시작일":(base+timedelta(90)).date(), "종료일":(base+timedelta(90)).date(), "비고":""})
    return pd.DataFrame(lst)

def ensure_data_types(df):
    df = df.copy()
    df["시작일"] = pd.to_datetime(df["시작일"], errors='coerce').dt.date
    df["종료일"] = pd.to_datetime(df["종료일"], errors='coerce').dt.date
    return df

def recalculate_dates(df, target):
    df["시작일"] = pd.to_datetime(df["시작일"]); df["종료일"] = pd.to_datetime(df["종료일"])
    mask = df["구분"].str.contains("최종 플루토 OK", na=False)
    if not mask.any(): return df
    idx = df[mask].index[0]
    curr = pd.to_datetime(target)
    df.at[idx, "종료일"] = curr
    df.at[idx, "시작일"] = curr - timedelta(days=max(0, int(df.at[idx, "소요 일수"])-1))
    
    link = df.at[idx, "시작일"]
    for i in range(idx-1, -1, -1):
        if df.at[i, "독립 일정"]: continue
        df.at[i, "종료일"] = link - timedelta(1)
        df.at[i, "시작일"] = df.at[i, "종료일"] - timedelta(days=max(0, int(df.at[i, "소요 일수"])-1))
        link = df.at[i, "시작일"]
    link = df.at[idx, "종료일"]
    for i in range(idx+1, len(df)):
        if df.at[i, "독립 일정"]: continue
        df.at[i, "시작일"] = link + timedelta(1)
        df.at[i, "종료일"] = df.at[i, "시작일"] + timedelta(days=max(0, int(df.at[i, "소요 일수"])-1))
        link = df.at[i, "종료일"]
    return df

def create_ics_file(df, title):
    c = ["BEGIN:VCALENDAR", "VERSION:2.0", f"X-WR-CALNAME:EBS {title}"]
    for _, r in df.iterrows():
        try:
            s = r['시작일'].strftime('%Y%m%d'); e = (pd.to_datetime(r['종료일']) + timedelta(1)).strftime('%Y%m%d')
            c.extend(["BEGIN:VEVENT", f"DTSTART;VALUE=DATE:{s}", f"DTEND;VALUE=DATE:{e}", f"SUMMARY:{r['구분']}", "END:VEVENT"])
        except: continue
    c.append("END:VCALENDAR")
    return "\n".join(c).encode('utf-8')

# --- 팝업 ---
@st.dialog("✨ 새로운 교재 등록")
def entry_dialog():
    c1,c2,c3=st.columns(3)
    y = c1.selectbox("연도", ["2025","2026","2027"])
    l = c2.selectbox("학교급", ["초등","중학","고교"])
    s = c3.selectbox("과목", ["국어","영어","수학","사회","과학"])
    ser = st.text_input("시리즈"); tit = st.text_input("교재명")
    if st.button("등록", type="primary"):
        if ser and tit:
            new_p = {"id":str(uuid.uuid4()), "year":y, "level":l, "subject":s, "series":ser, "title":tit, "schedule_data":create_initial_schedule(datetime.today()), "created_at":datetime.now(), "author_links":[], "reviewer_links":[], "partner_list":[], "settlement_list":[], "contract_status":{}, "book_specs":{}, "dev_data":pd.DataFrame(), "target_date_val":datetime.today()}
            st.session_state['projects'].append(new_p)
            save_current_state(); st.rerun()

@st.dialog("⚠️ 삭제 확인")
def delete_confirm_dialog(ids):
    st.warning(f"{len(ids)}개의 교재를 삭제하시겠습니까? (복구 불가)"); c1,c2=st.columns(2)
    if c1.button("🔴 삭제", type="primary"):
        st.session_state['projects'] = [p for p in st.session_state['projects'] if p['id'] not in ids]
        if st.session_state.get('current_project_id') in ids: st.session_state['current_project_id'] = None
        save_current_state(); st.rerun()
    if c2.button("취소"): st.rerun()

# --- HOME ---
if menu == "교재 등록 및 개요(HOME)":
    st.title("📊 교재 등록 및 개요")
    total = len(st.session_state['projects'])
    imp, done = 0, 0
    today = pd.Timestamp.now().normalize()
    for p in st.session_state['projects']:
        sch = p.get('schedule_data')
        if sch is not None and not sch.empty:
            for _, r in sch.iterrows():
                try:
                    ed = pd.to_datetime(r['종료일'])
                    if 0 <= (ed-today).days <= 3: imp += 1; break
                except: pass
        if get_schedule_date(p) and get_schedule_date(p) < today: done += 1
    
    c1,c2,c3 = st.columns(3)
    c1.metric("전체 교재", total); c2.metric("마감 임박", imp); c3.metric("완료", done)
    st.markdown("---")
    cL, cR = st.columns([1, 1.3])
    with cL:
        st.subheader("🔔 마감 임박")
        cnt = 0
        for p in st.session_state['projects']:
             sch = p.get('schedule_data')
             if sch is not None:
                 for _, r in sch.iterrows():
                     try:
                         ed = pd.to_datetime(r['종료일'])
                         if 0 <= (ed-today).days <= 3:
                             st.warning(f"[{p['title']}] {r['구분']} (D-{(ed-today).days})"); cnt+=1; break
                     except: pass
        if cnt==0: st.info("임박한 일정이 없습니다.")

    with cR:
        st.subheader("🛠️ 교재 관리")
        if st.button("✨ 신규 등록"): entry_dialog()
        rows = [{"선택": p['id']==st.session_state['selected_overview_id'], "삭제":False, "연도":p['year'], "교재명":p['title'], "ID":p['id']} for p in st.session_state['projects']]
        df = pd.DataFrame(rows)
        if not df.empty:
            edited = st.data_editor(df, hide_index=True, width="stretch", column_config={"선택":st.column_config.CheckboxColumn(width="small")})
            sel = edited[edited['선택']==True]
            if not sel.empty:
                pid = sel.iloc[0]['ID']
                if pid != st.session_state['selected_overview_id']:
                    st.session_state['selected_overview_id'] = pid
                    st.session_state['current_project_id'] = pid
                    st.rerun()
            del_rows = edited[edited['삭제']==True]
            if not del_rows.empty:
                if st.button("🗑️ 선택 삭제"): delete_confirm_dialog(del_rows['ID'].tolist())
    
    if st.session_state['selected_overview_id']:
        sel_p = get_project_by_id(st.session_state['selected_overview_id'])
        if sel_p:
            st.info(f"📌 선택됨: **{sel_p['title']}**")
            c_ov1, c_ov2 = st.columns(2)
            with c_ov1:
                auth_names = []
                for link in sel_p.get('author_links', []):
                    if link['person_id'] in st.session_state['people']:
                        auth_names.append(st.session_state['people'][link['person_id']]['이름'])
                st.caption(f"집필진: {', '.join(auth_names) if auth_names else '-'}")
            with c_ov2:
                sch = sel_p.get('schedule_data')
                if sch is not None and not sch.empty:
                    major = sch[sch['구분'].str.contains("🔴", na=False)]
                    if not major.empty:
                        for _,r in major.iterrows(): st.caption(f"{r['시작일']} : {r['구분']}")

# --- 1. 교재 관리 ---
elif menu == "1. 교재 관리":
    if not current_p: st.warning("교재를 선택해주세요."); st.stop()
    st.title(f"1. 교재 관리 - {current_p['title']}")
    t1, t2, t3 = st.tabs(["📊 배열표 관리", "🗓️ 일정 관리", "📕 교재 사양"])
    
    with t1:
        st.subheader("배열표 관리")
        c_d, c_u = st.columns([1, 2])
        with c_d:
            sample = pd.DataFrame({"대단원":["1.단원"],"중단원":["1.소단원"],"쪽수":[10],"집필자":["홍길동"]})
            st.download_button("⬇️ 양식 다운로드", sample.to_csv(index=False).encode('utf-8-sig'), "template.csv")
            st.caption("⚠️ 주의: '집필자' 컬럼의 이름이 인물 DB에 없으면 자동 생성됩니다.")
        
        with c_u:
            up = st.file_uploader("배열표 업로드 (Excel/CSV)", type=['xlsx','csv'])
            if up:
                try:
                    df = pd.read_csv(up) if up.name.endswith('.csv') else pd.read_excel(up)
                    current_p['planning_data'] = df
                    # [Sync Logic]
                    if '집필자' in df.columns:
                        existing_ids = [l['person_id'] for l in current_p.get('author_links', [])]
                        for name in df['집필자'].unique():
                            if not name or str(name) in ['nan','-']: continue
                            found_pid = None
                            for pid, info in st.session_state['people'].items():
                                if info['이름'] == name: found_pid = pid; break
                            if not found_pid:
                                found_pid = str(uuid.uuid4())[:8]
                                st.session_state['people'][found_pid] = {"이름": name, "소속": "자동등록"}
                            if found_pid not in existing_ids:
                                current_p['author_links'].append({"person_id": found_pid, "역할": "공동집필"})
                                existing_ids.append(found_pid)
                    st.success("업로드 및 인물 연동 완료")
                except: st.error("파일 읽기 실패")

        if st.button("🔄 데이터 연동 (수동)"): st.rerun()
        df = current_p.get('planning_data', pd.DataFrame(columns=["대단원","중단원","쪽수","집필자"]))
        edited = st.data_editor(df, num_rows="dynamic", width="stretch")
        if not edited.equals(df): current_p['planning_data'] = edited

    with t2:
        st.subheader("일정 관리")
        c1, c2 = st.columns([1, 2])
        with c1:
            st.download_button("⬇️ 일정 양식 다운로드", pd.DataFrame({"구분":["집필"],"시작일":["2025-01-01"],"종료일":["2025-01-31"]}).to_csv(index=False).encode('utf-8-sig'), "schedule_template.csv")
            up_sch = st.file_uploader("일정 업로드", type=['xlsx','csv'])
            if up_sch:
                try:
                    sdf = pd.read_csv(up_sch) if up_sch.name.endswith('.csv') else pd.read_excel(up_sch)
                    current_p['schedule_data'] = ensure_data_types(sdf)
                    st.success("일정 반영 완료")
                except: st.error("파일 오류")
        with c2:
            if st.button("⚡ 일정 자동 생성"): current_p['schedule_data'] = create_initial_schedule(datetime.today()); st.rerun()
            ics = create_ics_file(current_p.get('schedule_data', pd.DataFrame()), current_p['title'])
            st.download_button("📅 구글 캘린더용(ICS) 저장", ics, "schedule.ics")

        sch = current_p.get('schedule_data', pd.DataFrame())
        edited = st.data_editor(ensure_data_types(sch), num_rows="dynamic", width="stretch")
        if not edited.equals(sch): current_p['schedule_data'] = edited

    with t3:
        st.subheader("교재 사양")
        specs = current_p.get('book_specs', {})
        c_s1, c_s2 = st.columns(2)
        with c_s1:
            specs['format'] = st.text_input("판형", specs.get('format',''))
            specs['page_cnt'] = st.number_input("쪽수", value=specs.get('page_cnt', 0))
        with c_s2:
            specs['binding'] = st.text_input("제본 방식", specs.get('binding',''))
            specs['colors'] = st.multiselect("도수", ["1도","2도","4도"], default=specs.get('colors',[]))
        current_p['book_specs'] = specs

# --- 2. 참여자 관리 (4단 탭 구성) ---
elif menu == "2. 참여자 관리":
    if not current_p: st.warning("교재를 선택해주세요."); st.stop()
    st.title("2. 참여자 관리")
    
    t_auth, t_rev, t_part, t_master = st.tabs(["📝 집필진", "🔍 검토진", "🏢 참여 업체", "🗂️ 전체 인물 DB"])

    # 1. 집필진
    with t_auth:
        st.info("현재 교재의 집필진입니다. (마스터 DB와 연동)")
        auth_rows = []
        for i, link in enumerate(current_p.get('author_links', [])):
            pid = link['person_id']
            if pid in st.session_state['people']:
                info = st.session_state['people'][pid]
                auth_rows.append({"이름": info['이름'], "소속": info.get('소속',''), "연락처": info.get('연락처',''), "역할": link['역할'], "_idx": i})
        
        # Fallback
        for old in current_p.get('author_list', []):
            if isinstance(old, dict) and '이름' in old: auth_rows.append(old)

        df_auth = pd.DataFrame(auth_rows)
        if not df_auth.empty:
            c_list = st.columns([3, 1])
            with c_list[0]: st.dataframe(df_auth.drop(columns=['_idx'] if '_idx' in df_auth else []), width=800)
            with c_list[1]:
                del_idx = st.number_input("삭제할 행 번호", min_value=0, max_value=len(auth_rows)-1, step=1, key="del_auth_idx")
                if st.button("삭제", key="btn_del_auth"):
                    if 0 <= del_idx < len(current_p['author_links']):
                        del current_p['author_links'][del_idx]
                        save_current_state(); st.rerun()

        st.markdown("---")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### 🔍 기존 인물 검색 추가")
            all_people = {pid: format_person_label(info) for pid, info in st.session_state['people'].items()}
            sel_pid = st.selectbox("인물 선택", ["선택"] + list(all_people.keys()), format_func=lambda x: all_people.get(x,x), key="auth_sel")
            if st.button("추가", key="btn_add_auth_link"):
                if sel_pid != "선택":
                    current_p['author_links'].append({"person_id": sel_pid, "역할": "공동집필"})
                    save_current_state(); st.rerun()
        with c2:
            st.markdown("##### ✨ 신규 인물 등록")
            with st.form("new_auth"):
                n = st.text_input("이름"); a = st.text_input("소속"); p = st.text_input("연락처")
                if st.form_submit_button("등록 및 추가"):
                    if n:
                        pid = str(uuid.uuid4())[:8]
                        st.session_state['people'][pid] = {"이름": n, "소속": a, "연락처": p}
                        current_p['author_links'].append({"person_id": pid, "역할": "공동집필"})
                        save_current_state(); st.rerun()
# 2. 검토진 (매칭 정보 복구)
    with t_rev:
        st.info("현재 교재의 검토진입니다.")
        rev_rows = []
        for i, link in enumerate(current_p.get('reviewer_links', [])):
            pid = link['person_id']
            if pid in st.session_state['people']:
                info = st.session_state['people'][pid]
                rev_rows.append({"이름": info['이름'], "차수": link.get('검토차수',''), "범위": link.get('매칭정보',''), "_idx": i})
        
        # Fallback
        for old in current_p.get('reviewer_list', []):
             if isinstance(old, dict) and '이름' in old: rev_rows.append(old)

        df_rev = pd.DataFrame(rev_rows)
        if not df_rev.empty:
            c_list = st.columns([3, 1])
            with c_list[0]: st.dataframe(df_rev.drop(columns=['_idx'] if '_idx' in df_rev else []), width=800)
            with c_list[1]:
                del_idx_r = st.number_input("삭제할 행 번호", min_value=0, max_value=len(rev_rows)-1, step=1, key="del_rev_idx")
                if st.button("삭제", key="btn_del_rev"):
                    if 0 <= del_idx_r < len(current_p['reviewer_links']):
                        del current_p['reviewer_links'][del_idx_r]
                        save_current_state(); st.rerun()
        
        st.markdown("---")
        plan_df = current_p.get('planning_data', pd.DataFrame())
        unit_opts = []
        if not plan_df.empty:
            if '대단원' in plan_df.columns:
                plan_df['Key'] = plan_df.apply(lambda x: f"[{x.get('분권','')}] {x.get('대단원','')} > {x.get('중단원','')}", axis=1)
                unit_opts = plan_df['Key'].unique().tolist()
            elif '집필자' in plan_df.columns:
                unit_opts = plan_df['집필자'].unique().tolist()
        
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### 🔍 기존 인물 검색 추가")
            all_people_r = {pid: format_person_label(info) for pid, info in st.session_state['people'].items()}
            sel_pid_r = st.selectbox("인물 선택", ["선택"] + list(all_people_r.keys()), format_func=lambda x: all_people_r.get(x,x), key="rev_sel")
            role_r = st.selectbox("검토 차수", ["1차외부검토", "2차외부검토", "3차외부검토", "편집검토", "감수"], key="rev_role_sel")
            ranges_r = st.multiselect("검토 범위 (배열표 연동)", unit_opts, key="rev_range_sel")
            
            if st.button("추가", key="btn_add_rev_link"):
                if sel_pid_r != "선택":
                    current_p['reviewer_links'].append({"person_id": sel_pid_r, "검토차수": role_r, "매칭정보": ", ".join(ranges_r)})
                    save_current_state(); st.rerun()
        with c2:
            st.markdown("##### ✨ 신규 인물 등록")
            with st.form("new_rev"):
                n = st.text_input("이름"); a = st.text_input("소속"); p = st.text_input("연락처")
                ro = st.selectbox("차수", ["1차외부검토", "2차외부검토", "3차외부검토", "편집검토", "감수"])
                ra = st.multiselect("범위", unit_opts)
                if st.form_submit_button("등록 및 추가"):
                    if n:
                        pid = str(uuid.uuid4())[:8]
                        st.session_state['people'][pid] = {"이름": n, "소속": a, "연락처": p}
                        current_p['reviewer_links'].append({"person_id": pid, "검토차수": ro, "매칭정보": ", ".join(ra)})
                        save_current_state(); st.rerun()

    # 3. 업체
    with t_part:
        st.info("참여 업체 관리")
        pdf = pd.DataFrame(current_p.get('partner_list', []))
        cols = ["업체명", "분야", "담당자", "연락처", "비고"]
        if pdf.empty: pdf = pd.DataFrame(columns=cols)
        edited_p = st.data_editor(pdf, num_rows="dynamic", width="stretch", key="part_edit")
        if not edited_p.equals(pdf):
            current_p['partner_list'] = edited_p.to_dict('records')
            save_current_state()

    # 4. 마스터 DB
    with t_master:
        st.warning("⚠️ 전체 인물 DB입니다. 수정 시 모든 교재에 반영됩니다.")
        with st.expander("➕ 신규 인물 마스터 등록"):
            with st.form("master_add"):
                c1, c2, c3 = st.columns(3)
                n = c1.text_input("이름"); a = c2.text_input("소속"); p = c3.text_input("연락처")
                e = c1.text_input("이메일"); b = c2.text_input("은행"); acc = c3.text_input("계좌")
                if st.form_submit_button("DB 등록"):
                    if n:
                        pid = str(uuid.uuid4())[:8]
                        st.session_state['people'][pid] = {"이름":n, "소속":a, "연락처":p, "이메일":e, "은행명":b, "계좌번호":acc}
                        save_current_state(); st.success("등록됨"); st.rerun()
        
        st.write("---")
        query = st.text_input("🔍 검색 (이름/연락처)", placeholder="검색어를 입력하세요...")
        if query or st.button("전체 목록 보기"):
            res = []
            for pid, info in st.session_state['people'].items():
                if not query or (query in info['이름'] or query in str(info.get('연락처',''))):
                    r = info.copy(); r['ID'] = pid; res.append(r)
            
            df_res = pd.DataFrame(res)
            cols = ["이름", "소속", "연락처", "이메일", "은행명", "계좌번호", "주소", "ID"]
            if df_res.empty: df_res = pd.DataFrame(columns=cols)
            else: 
                for c in cols: 
                    if c not in df_res.columns: df_res[c] = ""

            edited = st.data_editor(df_res[cols], num_rows="dynamic", width="stretch", column_config={"ID": st.column_config.TextColumn(disabled=True)})
            if st.button("변경사항 저장"):
                for _, row in edited.iterrows():
                    pid = row['ID']
                    if pd.isna(pid) or not pid: pid = str(uuid.uuid4())[:8]
                    d = row.to_dict(); del d['ID']
                    st.session_state['people'][pid] = d
                save_current_state(); st.success("저장 완료")

# --- 3. 집필 및 검토 관리 ---
elif menu == "3. 집필 및 검토 관리":
    if not current_p: st.warning("교재 선택 필요"); st.stop()
    st.title("3. 집필 및 검토 관리")
    t1, t2 = st.tabs(["👥 배정 및 진행", "🔄 자동 매칭"])
    
    with t1:
        st.subheader("단원별 담당자 매트릭스")
        dev_df = current_p['dev_data']
        # 보기 편하게 컬럼 필터링
        base_cols = ["단원명", "집필자"]
        assign_cols = [c for c in dev_df.columns if "완료" not in c and "상태" not in c and c not in base_cols and c != "비고"]
        
        ordered = []
        for role in ["1차", "2차", "3차", "편집", "감수"]:
            for c in assign_cols:
                if role in c and c not in ordered: ordered.append(c)
        remaining = [c for c in assign_cols if c not in ordered]
        
        edited = st.data_editor(dev_df[base_cols + ordered + remaining], hide_index=True, width="stretch", key="dev_mat_edit")
        if not edited.equals(dev_df[base_cols + ordered + remaining]):
            dev_df.update(edited); current_p['dev_data'] = dev_df
        
        st.subheader("상세 진행 관리")
        req_cols = ["단원명", "집필자", "집필완료", "1차검토완료", "2차검토완료", "3차검토완료", "편집검토완료"]
        for c in req_cols:
             if c not in dev_df.columns: dev_df[c] = False
        
        edited_s = st.data_editor(dev_df[req_cols], hide_index=True, width="stretch", key="dev_stat_edit")
        if not edited_s.equals(dev_df[req_cols]):
            dev_df.update(edited_s); current_p['dev_data'] = dev_df

    with t2:
        if st.button("🔄 검토자 자동 배정 (매칭 정보 기반)", type="primary"):
            dev_df = current_p['dev_data']
            # 매칭 로직
            cnt = 0
            for r in current_p.get('reviewer_links', []):
                # 인물 DB에서 이름 가져오기
                if r['person_id'] in st.session_state['people']:
                    r_name = st.session_state['people'][r['person_id']]['이름']
                    targets = [t.strip() for t in str(r.get('매칭정보','')).split(',')]
                    col = normalize_string(r.get('검토차수',''))
                    
                    if col and col not in dev_df.columns: dev_df[col] = "-"
                    if col in dev_df.columns:
                        for idx, row in dev_df.iterrows():
                            u_name = str(row['단원명'])
                            # 단순 포함 여부 체크
                            if any(t in u_name for t in targets):
                                cur = str(dev_df.at[idx, col])
                                if cur in ['-', 'nan', '']: dev_df.at[idx, col] = r_name
                                elif r_name not in cur: dev_df.at[idx, col] = cur + ", " + r_name
                                cnt += 1
            current_p['dev_data'] = dev_df
            st.success(f"{cnt}건 배정 완료"); st.rerun()

# --- 4. 정산 ---
elif menu == "4. 개발 후 관리(정산 및 결과 보고)":
    if not current_p: st.warning("교재 선택 필요"); st.stop()
    st.title("4. 개발 후 관리")
    tab_settle, tab_report = st.tabs(["💰 정산", "📑 결과보고서"])
    
    with tab_settle:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("###### 집필료 기준")
            e_auth = st.data_editor(current_p['author_standards'], num_rows="fixed", hide_index=True)
            if not e_auth.equals(current_p['author_standards']): current_p['author_standards'] = e_auth
        with c2:
            st.markdown("###### 검토료 기준")
            e_rev = st.data_editor(current_p['review_standards'], num_rows="dynamic", hide_index=True)
            if not e_rev.equals(current_p['review_standards']): current_p['review_standards'] = e_rev
        
        st.markdown("---")
        if st.button("🔄 자동 산출 (데이터 연동)", type="primary"):
            # 정산 로직 복구
            plan_df = current_p.get('planning_data', pd.DataFrame())
            auth_std = current_p['author_standards']
            new_settle = []
            
            if not plan_df.empty:
                plan_df['쪽수'] = pd.to_numeric(plan_df.get('쪽수',0), errors='coerce').fillna(0)
                plan_df['문항수'] = pd.to_numeric(plan_df.get('문항수',0), errors='coerce').fillna(0)
                if '집필자' in plan_df.columns:
                    ag = plan_df.groupby('집필자')[['쪽수','문항수']].sum().reset_index()
                    for _, row in ag.iterrows():
                        name = row['집필자']
                        if name and str(name) not in ['nan','-']:
                            try: p_p = int(auth_std.iloc[0]['원고료'])
                            except: p_p = 0
                            try: p_i = int(auth_std.iloc[1]['원고료'])
                            except: p_i = 0
                            
                            if row['쪽수']>0: new_settle.append({"구분":"집필", "이름":name, "내용":"원고(쪽)", "수량":row['쪽수'], "단가":p_p, "공급가액":row['쪽수']*p_p})
                            if row['문항수']>0: new_settle.append({"구분":"집필", "이름":name, "내용":"원고(문항)", "수량":row['문항수'], "단가":p_i, "공급가액":row['문항수']*p_i})
            
            # 검토자 정산 추가
            for link in current_p.get('reviewer_links', []):
                 if link['person_id'] in st.session_state['people']:
                     r_name = st.session_state['people'][link['person_id']]['이름']
                     new_settle.append({"구분":"검토", "이름":r_name, "내용":link['검토차수'], "수량":0, "단가":0, "공급가액":0})

            current_p['settlement_list'] = new_settle
            st.rerun()

        sdf = pd.DataFrame(current_p['settlement_list'])
        if sdf.empty: sdf = pd.DataFrame(columns=["구분","이름","내용","수량","단가","공급가액"])
        edited_s = st.data_editor(sdf, num_rows="dynamic", width="stretch")
        if not edited_s.equals(sdf):
            edited_s['수량'] = safe_to_numeric(edited_s['수량'])
            edited_s['단가'] = safe_to_numeric(edited_s['단가'])
            edited_s['공급가액'] = edited_s['수량'] * edited_s['단가']
            current_p['settlement_list'] = edited_s.to_dict('records')
            st.rerun()
            
        total = edited_s['공급가액'].sum() if not edited_s.empty else 0
        st.metric("총 지급액", f"{int(total):,}원")

    with tab_report:
        chk = current_p['report_checklist']
        e_chk = st.data_editor(chk, width="stretch")
        if not e_chk.equals(chk): current_p['report_checklist'] = e_chk

# --- 5. 약정서 관리 ---
elif menu == "5. 약정서 관리":
    if not current_p: st.warning("교재 선택 필요"); st.stop()
    st.title("5. 약정서 관리")
    t1, t2 = st.tabs(["검토약정서", "집필약정서"])
    
    with t1:
        c_L, c_R = st.columns([1, 2])
        with c_L:
            # DB Select
            all_p = {pid: format_person_label(info) for pid, info in st.session_state['people'].items()}
            sel = st.selectbox("대상 선택", ["직접 입력"] + list(all_p.keys()), format_func=lambda x: all_p.get(x,x))
            
            if sel == "직접 입력": name_val = st.text_input("이름")
            else: name_val = st.session_state['people'][sel]['이름']
            
            role_val = st.text_input("검토 차수", "1차외부검토", key="contract_role_main_input")
            
            up_sig = st.file_uploader("직인 업로드", type=['png','jpg'], key="sig_up")
            if up_sig: current_p['dept_head_sig'] = up_sig.getvalue()
            if current_p.get('dept_head_sig'): st.image(current_p['dept_head_sig'], width=100)

        with c_R:
            fee = st.number_input("금액", step=10000)
            d1 = st.date_input("시작일"); d2 = st.date_input("종료일")
            
            if st.button("미리보기"):
                preview_contract_dialog({"name":name_val, "book_title":current_p['title'], "role":role_val, "fee":fee, "period":f"{d1}~{d2}", "date":str(datetime.today().date()), "dept_head":"부장"})
            
            if st.button("링크 생성"):
                lbl = f"[{role_val}] {name_val}"
                current_p['contract_status'][lbl] = {"link_token":str(uuid.uuid4())[:8], "status":"Link Sent", "name":name_val, "role":role_val, "final_fee":fee, "start_date":d1, "end_date":d2, "dept_head":"부장", "special_note":""}
                save_current_state(); st.success("생성 완료"); st.rerun()
        
        st.markdown("---")
        for i, (k, v) in enumerate(current_p['contract_status'].items()):
            c1, c2, c3 = st.columns([2, 1, 1])
            c1.write(f"**{k}**"); c2.write(v['status'])
            if c3.button("접속", key=f"btn_con_{i}"):
                st.session_state['view_mode'] = 'reviewer'
                st.session_state['active_token'] = v['link_token']
                st.rerun()