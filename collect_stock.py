import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import FinanceDataReader as fdr
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os

# .env 파일 로드 (로컬에서만 사용)
try:
    from dotenv import load_dotenv
    # 루트 디렉토리의 .env 파일 로드
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(root_dir, '.env')
    print(f"env_path : {env_path}")
    
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path, override=True)
except ImportError:
    pass

# 데이터베이스 연결 URL (.env 파일의 DATABASE_URL 사용)
TARGET_DB_URL = os.environ.get('DATABASE_URL')
print(f" [DB] TARGET_DB_URL: {TARGET_DB_URL}")

def create_tables(engine):
    """주어진 엔진으로 stocks, daily_prices 테이블을 생성합니다."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stocks (
                ticker VARCHAR(10) PRIMARY KEY,
                company_name VARCHAR(100) NOT NULL,
                market VARCHAR(20),
                market_cap BIGINT
            );
        """))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_prices (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(10) REFERENCES stocks(ticker),
                date DATE NOT NULL,
                open INT NOT NULL,
                high INT NOT NULL,
                low INT NOT NULL,
                close INT NOT NULL,
                volume BIGINT NOT NULL,
                CONSTRAINT unique_ticker_date UNIQUE (ticker, date)
            );
        """))
        
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_prices_ticker_date 
            ON daily_prices (ticker, date DESC);
        """))

        # market_cap 컬럼이 없는 경우 추가 (기존 DB 마이그레이션)
        conn.execute(text("""
            ALTER TABLE stocks ADD COLUMN IF NOT EXISTS market_cap BIGINT;
        """))
        conn.commit()
    print(" [DB] 테이블 및 인덱스 스키마 확인 완료.")


def ensure_tables(engine):
    """이미 연결된 엔진으로 테이블만 생성 (Cron Job에서 사용)."""
    create_tables(engine)


def init_database():
    """데이터베이스 연결을 초기화합니다. .env 파일의 DATABASE_URL을 사용합니다."""
    if not TARGET_DB_URL:
        raise ValueError("DATABASE_URL 환경변수가 설정되지 않았습니다. .env 파일을 확인하세요.")
    
    # DATABASE_URL로 직접 연결
    target_engine = create_engine(TARGET_DB_URL)
    create_tables(target_engine)
    
    # 시퀀스를 현재 최대 ID로 리셋 (중복 ID 방지)
    with target_engine.connect() as conn:
        result = conn.execute(text("SELECT MAX(id) FROM daily_prices"))
        max_id = result.fetchone()[0]
        if max_id:
            conn.execute(text(f"SELECT setval('daily_prices_id_seq', {max_id}, true)"))
            conn.commit()
            print(f" [DB] 시퀀스 리셋 완료: {max_id}")
    
    return target_engine

def get_last_dates_for_all_tickers(engine, tickers):
    """모든 ticker의 마지막 수집일을 한 번에 조회하여 dict로 반환합니다."""
    if not tickers:
        return {}
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT ticker, MAX(date) FROM daily_prices WHERE ticker = ANY(:tickers) GROUP BY ticker"),
            {"tickers": list(tickers)}
        )
        return {row[0]: row[1] for row in result.fetchall()}

def is_db_empty(engine):
    """daily_prices 테이블에 데이터가 하나도 없는지 확인합니다."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM daily_prices"))
        return result.fetchone()[0] == 0

def collect_incremental_data(engine, end_date, tickers):
    """
    증분 수집: 각 ticker별로 DB에 마지막으로 저장된 날짜 이후의 데이터만 수집합니다.
    오늘 데이터가 이미 있으면 건너뜁니다.
    마지막 수집일을 한 번에 조회하고, API 호출은 병렬로 처리합니다.
    """
    total_inserted_rows = 0
    skipped_count = 0
    total_processed = 0
    chunk_size = 50  # 진행 상황 표시 빈도 조정

    # 한 번에 모든 ticker의 마지막 날짜 조회 (DB 쿼리 1회)
    last_date_map = get_last_dates_for_all_tickers(engine, tickers)

    # 수집이 필요한 종목만 필터링
    today = datetime.date.today()
    tickers_to_fetch = []
    for ticker in tickers:
        last_date = last_date_map.get(ticker)
        if last_date and last_date >= today:
            skipped_count += 1
        else:
            tickers_to_fetch.append(ticker)

    print(f" [수집] 전체 {len(tickers)}종목 중 {len(tickers_to_fetch)}종목 수집 필요, {skipped_count}종목 건너뜀")

    # 각 ticker별 시작일 계산
    ticker_start_dates = {}
    for ticker in tickers_to_fetch:
        last_date = last_date_map.get(ticker)
        if last_date:
            ticker_start_dates[ticker] = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            ticker_start_dates[ticker] = (today - relativedelta(months=6)).strftime('%Y-%m-%d')

    def fetch_ticker_data(ticker):
        """단일 ticker 데이터 조회 (재시도 로직 포함)"""
        start_date = ticker_start_dates[ticker]
        for retry in range(3):
            try:
                df = fdr.DataReader(ticker, start=start_date, end=end_date)
                return ticker, df, None
            except Exception as e:
                print(f" [재시도 {retry+1}/3] {ticker} 데이터 수집 실패: {e}")
                if retry < 2:
                    time.sleep(2)
        return ticker, None, f"Failed after 3 retries"

    # 병렬 API 호출 (max_workers=5로 Rate Limit 방지)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_ticker_data, ticker): ticker for ticker in tickers_to_fetch}

        for future in as_completed(futures):
            ticker, df, error = future.result()
            total_processed += 1

            if error or df is None or df.empty:
                print(f"  [{ticker}] 데이터 없음 또는 수집 실패")
                continue

            df = df.reset_index()
            df['ticker'] = ticker
            df = df[['ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            df.columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']

            try:
                df.to_sql(
                    'daily_prices',
                    con=engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=100
                )
                total_inserted_rows += len(df)
                print(f"  [{ticker}] {len(df)}건 삽입 완료")
            except Exception as db_err:
                print(f" [DB 오류] {ticker} 저장 실패: {db_err}")

            del df

            # 진행 상황 표시
            if total_processed % chunk_size == 0 or total_processed == len(tickers_to_fetch):
                print(f" [진행] {total_processed}/{len(tickers_to_fetch)} 완료 (누적 {total_inserted_rows}건 적재)")

    print(f" [완료] 총 {total_inserted_rows}건의 새 데이터가 DB에 추가되었습니다. (건너뛴 종목: {skipped_count})")


def collect_all_data(engine, start_date, end_date, tickers):
    """
    전체 수집: 최초 1회용. 6개월치 데이터를 한꺼번에 수집합니다.
    API 호출은 병렬로 처리하여 수집 시간을 단축합니다.
    """
    total_inserted_rows = 0
    total_processed = 0
    chunk_size = 50  # 진행 상황 표시 빈도 조정

    def fetch_ticker_data(ticker):
        """단일 ticker 데이터 조회 (재시도 로직 포함)"""
        for retry in range(3):
            try:
                df = fdr.DataReader(ticker, start=start_date, end=end_date)
                return ticker, df, None
            except Exception as e:
                print(f" [재시도 {retry+1}/3] {ticker} 데이터 수집 실패: {e}")
                if retry < 2:
                    time.sleep(2)
        return ticker, None, f"Failed after 3 retries"

    # 병렬 API 호출 (max_workers=5)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_ticker_data, ticker): ticker for ticker in tickers}

        for future in as_completed(futures):
            ticker, df, error = future.result()
            total_processed += 1

            if error or df is None or df.empty:
                print(f"  [{ticker}] 데이터 없음")
                continue

            df = df.reset_index()
            df['ticker'] = ticker
            df = df[['ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            df.columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']

            try:
                df.to_sql(
                    'daily_prices',
                    con=engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=100
                )
                total_inserted_rows += len(df)
                print(f"  [{ticker}] {len(df)}건 삽입 완료")
            except Exception as db_err:
                print(f" [DB 오류] {ticker} 저장 실패: {db_err}")

            del df

            # 진행 상황 표시
            if total_processed % chunk_size == 0 or total_processed == len(tickers):
                print(f" [진행] {total_processed}/{len(tickers)} 완료 (누적 {total_inserted_rows}건 적재)")

    print(f" [완료] 전체 수집 프로세스 종료! 총 {total_inserted_rows}개의 일봉 데이터가 'stockdb'에 반영되었습니다.")

def delete_old_data(engine, retention_months=18):
    """
    18개월 이상 지난 daily_prices 데이터를 삭제합니다.
    증분 수집 전에 호출하여 오래된 데이터를 정리합니다.
    """
    cutoff_date = datetime.date.today() - relativedelta(months=retention_months)
    cutoff_str = cutoff_date.strftime('%Y-%m-%d')

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM daily_prices WHERE date < :cutoff"),
            {"cutoff": cutoff_str}
        )
        count = result.fetchone()[0]

        if count > 0:
            conn.execute(
                text("DELETE FROM daily_prices WHERE date < :cutoff"),
                {"cutoff": cutoff_str}
            )
            conn.commit()
            print(f" [정리] {cutoff_str} 이전 데이터 {count}건 삭제 완료. (보관 기간: {retention_months}개월)")
        else:
            print(f" [정리] {cutoff_str} 이전 데이터가 없어 삭제하지 않았습니다.")
    return count


def collect_market_data(engine, market_name, today, end_date):
    """
    특정 마켓(KOSPI/KOSDAQ)의 종목 마스터 정보를 저장하고 ticker 리스트를 반환합니다.
    이미 존재하는 종목은 시가총액을 업데이트합니다 (UPSERT).
    """
    df_listing = fdr.StockListing(market_name)
    stocks_master = df_listing[['Code', 'Name', 'Marcap']].copy()
    stocks_master.columns = ['ticker', 'company_name', 'market_cap']
    stocks_master['market'] = market_name
    stocks_master['market_cap'] = stocks_master['market_cap'].fillna(0).astype('int64')

    # UPSERT: ON CONFLICT 시 company_name, market, market_cap 업데이트
    total = len(stocks_master)
    chunk_size = 200  # 진행률 표시 간격
    with engine.connect() as conn:
        for idx, (_, row) in enumerate(stocks_master.iterrows()):
            conn.execute(
                text("""
                    INSERT INTO stocks (ticker, company_name, market, market_cap)
                    VALUES (:ticker, :company_name, :market, :market_cap)
                    ON CONFLICT (ticker) DO UPDATE SET
                        company_name = EXCLUDED.company_name,
                        market = EXCLUDED.market,
                        market_cap = EXCLUDED.market_cap
                """),
                {"ticker": row['ticker'], "company_name": row['company_name'],
                 "market": row['market'], "market_cap": row['market_cap']}
            )
            if (idx + 1) % chunk_size == 0 or (idx + 1) == total:
                print(f" [진행] {market_name} 종목 정보 업데이트: {idx + 1}/{total} ({((idx + 1) / total * 100):.0f}%)")
        conn.commit()
    print(f" [DB] {total}개의 {market_name} 종목 마스터 정보 업데이트 완료. (UPSERT)")

    return stocks_master['ticker'].tolist()


def collect_and_save_stock_data(engine, batch_size=None, offset=0):
    """
    DB 상태에 따라 최초 전체 수집 또는 증분 수집을 실행합니다.
    batch_size와 offset이 주어지면 해당 범위의 종목만 처리합니다.
    """
    today = datetime.date.today()
    end_date = today.strftime('%Y-%m-%d')

    print(f" [수집] 기준일: {end_date}")

    # KOSPI + KOSDAQ 종목 마스터 정보 수집 및 저장
    tickers = []
    tickers += collect_market_data(engine, 'KOSPI', today, end_date)
    tickers += collect_market_data(engine, 'KOSDAQ', today, end_date)

    print(f" [수집] 전체 수집 대상 종목 수: {len(tickers)}개 (KOSPI + KOSDAQ)")

    # 배치 처리: offset과 batch_size로 범위 제한
    if batch_size is not None:
        total_tickers = len(tickers)
        end_idx = min(offset + batch_size, total_tickers)
        tickers = tickers[offset:end_idx]
        print(f" [배치] {offset}~{end_idx}/{total_tickers} 종목 처리")

    # DB가 비어있으면 최초 전체 수집, 아니면 증분 수집
    if is_db_empty(engine):
        print(" [DB] daily_prices 테이블이 비어 있습니다. 최초 6개월 전체 수집을 진행합니다.")
        six_months_ago = today - relativedelta(months=6)
        start_date = six_months_ago.strftime('%Y-%m-%d')
        collect_all_data(engine, start_date, end_date, tickers)
        return {"mode": "full", "processed": len(tickers)}
    else:
        print(" [DB] 기존 데이터가 있습니다. 증분 수집을 진행합니다.")
        # 증분 수집 전 18개월 이상 지난 데이터 삭제
        delete_old_data(engine, retention_months=18)
        collect_incremental_data(engine, end_date, tickers)
        return {"mode": "incremental", "processed": len(tickers)}

if __name__ == "__main__":
    import sys
    
    # 명령줄 인자 확인
    batch_size = None
    offset = 0
    
    if len(sys.argv) > 1:
        try:
            batch_size = int(sys.argv[1])
            offset = int(sys.argv[2]) if len(sys.argv) > 2 else 0
            print(f" [옵션] 배치 모드: 크기={batch_size}, 시작={offset}")
        except ValueError:
            print(" [오류] 배치 크기와 오프셋은 숫자여야 합니다.")
            sys.exit(1)
    
    # 데이터베이스 상태 빌드 및 엔진 인스턴스 확보
    db_engine = init_database()
    # 주가 수집 및 저장 실행
    collect_and_save_stock_data(db_engine, batch_size=batch_size, offset=offset)
