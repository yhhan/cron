import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import FinanceDataReader as fdr
from sqlalchemy import create_engine, text
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

def get_last_date_for_ticker(engine, ticker):
    """해당 ticker의 DB 마지막 수집일을 조회합니다. 없으면 None 반환."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT MAX(date) FROM daily_prices WHERE ticker = :ticker"),
            {"ticker": ticker}
        )
        row = result.fetchone()
        return row[0] if row and row[0] else None

def is_db_empty(engine):
    """daily_prices 테이블에 데이터가 하나도 없는지 확인합니다."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM daily_prices"))
        return result.fetchone()[0] == 0

def collect_incremental_data(engine, end_date, tickers):
    """
    증분 수집: 각 ticker별로 DB에 마지막으로 저장된 날짜 이후의 데이터만 수집합니다.
    오늘 데이터가 이미 있으면 건너뜁니다.
    메모리 사용량 최소화를 위해 종목별 개별 처리.
    """
    total_inserted_rows = 0
    skipped_count = 0
    duplicate_count = 0
    chunk_size = 50  # 진행 상황 표시 빈도 조정

    for idx, ticker in enumerate(tickers):
        last_date = get_last_date_for_ticker(engine, ticker)

        # 오늘 데이터가 이미 있으면 건너뜀
        if last_date and last_date >= datetime.date.today():
            skipped_count += 1
            if (idx + 1) % chunk_size == 0 or (idx + 1) == len(tickers):
                print(f" [진행] {idx + 1}/{len(tickers)} 완료 (누적 {total_inserted_rows}건 적재, 건너뛴 종목: {skipped_count}, 중복: {duplicate_count})")
            continue

        # 시작일 결정: DB에 데이터가 있으면 마지막 날짜의 다음날, 없으면 6개월 전
        if last_date:
            ticker_start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            six_months_ago = datetime.date.today() - relativedelta(months=6)
            ticker_start_date = six_months_ago.strftime('%Y-%m-%d')

        # 재시트 로직: SSL/Network 에러 시 최대 3회 재시도
        df_price = None
        for retry in range(3):
            try:
                df_price = fdr.DataReader(ticker, start=ticker_start_date, end=end_date)
                break
            except Exception as e:
                print(f" [재시도 {retry+1}/3] {ticker} 데이터 수집 실패: {e}")
                if retry < 2:
                    time.sleep(2)
                continue

        if df_price is None or df_price.empty:
            continue

        df_price = df_price.reset_index()
        df_price['ticker'] = ticker
        df_price = df_price[['ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df_price.columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']

        # 개별 종목 즉시 저장 (메모리 누적 방지, bulk insert 사용)
        try:
            # to_sql을 사용한 bulk insert로 성능 개선
            df_price.to_sql(
                'daily_prices',
                con=engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=100
            )
            # 실제로 삽입된 행 수를 확인하기 위해 DB에서 카운트
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT COUNT(*) FROM daily_prices WHERE ticker = :ticker AND date >= :start_date"),
                    {"ticker": ticker, "start_date": ticker_start_date}
                )
                db_count = result.fetchone()[0]
            
            # 중복 데이터 수 계산
            duplicate_rows = len(df_price) - db_count
            if duplicate_rows > 0:
                duplicate_count += duplicate_rows
            total_inserted_rows += db_count
            
            print(f"  [{ticker}] {len(df_price)}건 시도, {db_count}건 삽입 (중복 {duplicate_rows}건)")
        except Exception as db_err:
            print(f" [DB 오류] {ticker} 저장 실패: {db_err}")

        # 메모리 해제
        del df_price

        # API 과부하 방지를 위한 짧은 대기
        time.sleep(0.05)

        if (idx + 1) % chunk_size == 0 or (idx + 1) == len(tickers):
            print(f" [진행] {idx + 1}/{len(tickers)} 완료 (누적 {total_inserted_rows}건 적재, 건너뛴 종목: {skipped_count}, 중복: {duplicate_count})")

    print(f" [완료] 총 {total_inserted_rows}건의 새 데이터가 DB에 추가되었습니다. (건너뛴 종목: {skipped_count}, 중복 데이터: {duplicate_count})")


def collect_all_data(engine, start_date, end_date, tickers):
    """
    전체 수집: 최초 1회용. 6개월치 데이터를 한꺼번에 수집합니다.
    메모리 사용량 최소화를 위해 종목별 개별 처리.
    """
    total_inserted_rows = 0
    chunk_size = 50  # 진행 상황 표시 빈도 조정

    for idx, ticker in enumerate(tickers):
        # 재시도 로직: SSL/Network 에러 시 최대 3회 재시도
        df_price = None
        for retry in range(3):
            try:
                df_price = fdr.DataReader(ticker, start=start_date, end=end_date)
                break
            except Exception as e:
                print(f" [재시도 {retry+1}/3] {ticker} 데이터 수집 실패: {e}")
                if retry < 2:
                    time.sleep(2)
                continue

        if df_price is None or df_price.empty:
            continue

        df_price = df_price.reset_index()
        df_price['ticker'] = ticker
        df_price = df_price[['ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df_price.columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']

        # 개별 종목 즉시 저장 (메모리 누적 방지)
        try:
            df_price.to_sql(
                'daily_prices',
                con=engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=100
            )
            total_inserted_rows += len(df_price)
            print(f"  [{ticker}] {len(df_price)}건 삽입 완료")
        except Exception as db_err:
            print(f" [DB 오류] {ticker} 저장 실패: {db_err}")

        # 메모리 해제
        del df_price

        # API 과부하 방지를 위한 짧은 대기
        time.sleep(0.05)

        if (idx + 1) % chunk_size == 0 or (idx + 1) == len(tickers):
            print(f" [진행] {idx + 1}/{len(tickers)} 완료 (누적 {total_inserted_rows}건 적재)")

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
