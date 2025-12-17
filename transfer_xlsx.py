def sheet_payload(df, raw_cols, n=30, date_col="date"):
    df = df.sort_values(date_col)
    df[date_col] = pd.to_datetime(df[date_col])

    last = df.iloc[-1]
    as_of = last[date_col].strftime("%Y-%m-%d")

    latest = {
        c: float(last[c])
        for c in df.columns if c != date_col and pd.notna(last[c])
    }

    raw = {
        c: [
            [d.strftime("%Y-%m-%d"), float(v)]
            for d, v in zip(df[date_col].tail(n), df[c].tail(n))
            if pd.notna(v)
        ]
        for c in raw_cols if c in df.columns
    }

    return {"as_of": as_of, "latest_metrics": latest, "raw_series": raw}


def xlsx_to_payload(path, raw_map, n=30):
    xls = pd.ExcelFile(path)
    return {
        "meta": {"as_of": datetime.today().strftime("%Y-%m-%d")},
        "indicators": {
            s: sheet_payload(
                pd.read_excel(xls, s),
                raw_map.get(s, []),
                n
            )
            for s in xls.sheet_names
        }
    }
