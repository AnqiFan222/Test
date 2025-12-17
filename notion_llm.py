import requests
import json
from datetime import datetime
from converter import markdown_to_notion_blocks
from gcs_excel import download_from_gcs
import requests
import json
import pandas as pd
from transfer_xlsx import sheet_payload, xlsx_to_payload

GCS_PATH = "gs://angel-project/reports/output.xlsx"
LOCAL_PATH = "/tmp/output.xlsx"

download_from_gcs(GCS_PATH, LOCAL_PATH)

raw_map = {"rrp": ["RRP_balance"],
           "srf": ["SRF_balance"],
           "walcl": ["walcl"],
           "tga": ["tga"],
           "ig":["IG_OAS"],
           "hy":["HY_OAS"],
           "se":["SOFR-EFFR"],
           "stress":["stress_index"]}

out = xlsx_to_payload(LOCAL_PATH, raw_map, n=30)
with open("llm_payload.json", "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, indent=2)
json_text = json.dumps(out, ensure_ascii=False, indent=2)

API_KEY = "sk-5d151ba715db4c9b85ba872433bcea3a"
API_URL = "https://api.deepseek.com/v1/chat/completions"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

payload = {
    "model": "deepseek-chat",
    "messages": [
        {"role": "system", "content": f"""使用用户上传的JSON数据{json_text}，生成每日流动性与市场压力日报。请基于数据完成任务，输出日报的格式为markdown,每个标题要用markdown格式标清楚大小次序。
         日报必须严格按照固定结构输出，分为两个大标题（资金供应端、市场压力端）、共七个自然段，每自然段有个小标题，每段150–200字，逻辑连贯、分析精准，不罗列数据、不定义指标。

结构如下：

一、资金供应端（共三段）
1. RRP（Reverse Repo Balance），单位为“亿美元”，分析余额变化及其对流动性松紧的信号，注明日期。注意：RRP 的原始数据单位为“十亿美元”，若数值为 6，应换算为“60 亿美元”。
2. SRF（Standing Repo Facility），单位为“亿美元”，分析银行体系短期资金压力与流动性需求，注明日期。注意：SRF 的原始数据单位为“十亿美元”，若数值为 6，应换算为“60 亿美元”。
3. WALCL（美联储资产负债表总资产）与 TGA（财政部一般账户）合并分析，单位为“百万美元”，须在文中明确说明其为周度指标，结合资产负债表扩缩与财政收支对流动性投放或回笼的综合影响，注明日期。

二、市场压力端（共四段）
1. HY-Spread（高收益债利差），单位%，分析市场风险偏好变化。
2. IG-Spread（投资级债利差），单位%，评估机构风险定价与市场稳定性。
3. SOFR-Spread（SOFR–EFFR 利差），单位%，观察短端资金供需变化与钱荒风险。
4. Stress Index（金融市场压力指数），判断系统性金融压力。

写作要求：
- 每段150–200字，语气稳健、专业；
- 两个主题之间以主题名称作分隔行（如“【资金供应端】”“【市场压力端】”），其余部分不出现小标题；
- WALCL与TGA为周度指标，须在对应段落中明确说明；
- 所有涉及日期的分析必须明确说明时间区间，例如“2025年11月27日至11月28日”；
- 若仅提供单日数据，系统自动与前一日或上一期比较并注明时间；
- 分析必须结合历史时间序列数据的走势，重点参考过去一周至一月的连续数据点，识别趋势变化与波动模式，不能仅依赖均值、分位、波动率、z-score等静态指标；
- 长期趋势判断可结合年内时间序列演变轨迹，明确指出当前数值处于全年分布的相对位置；
- 不得机械罗列或解释技术指标定义，而应将其用于支撑逻辑判断；
- 避免使用模糊时间表述，如“近日”“近期”“近几天”；
- 不得使用模糊结论或“建议式”表达，如“仍需观察”“反映出……之间的拉锯”“建议继续观测”等；
- 分析必须以数据变化为逻辑主线，结论落点明确，例如“显示流动性边际收紧”或“反映资金面趋稳”；
- 语气冷静、客观、直接，不使用不确定或空洞性语句；
- 引用当日数据并注明日期与单位；
- 根据变动幅度判断重点；
- 若数据缺失，以谨慎语气逻辑补充；
- 输出仅包含上述两部分共七段正文，格式整洁规范，可直接用于日报发布；
- 所有金额单位应使用中文表达：“亿美元”代表十亿级，“百万美元”代表百万级，不使用Bn或Mln英文缩写；系统应根据段落设定单位进行统一换算和表述，不得擅自扩大或缩小数量级；
- 严格将 billion 解析为 10 亿（即 1B = 10^9），不得错误理解为 hundred billion 或乘以100，不得将如“7.561B”错误换算为“7561 亿美元”，应为“75.61 亿美元”；
- RRP 和 SRF 数据单位原始为“十亿美元”，所有文本输出必须乘以10后以“亿美元”为单位表达，并精确至0.01亿；
- 模型必须明确知悉：输入数据中未直接标注“B”或“M”等单位，单位解释完全依据段落标准进行：1）RRP（Reverse Repo Balance）为“十亿美元”，2）SRF（Standing Repo Facility）为“十亿美元”，3）WALCL（美联储资产负债表总资产）与TGA（财政部一般账户）为“百万美元”。所有换算必须依据该规则，不得根据数字本身作出单位推测；
- 模型必须严格引用表格中明确列示的数据日期（data date），不得以系统日期代替；当表格日期与自然日不一致时，以表格中所示数据日为主，分析中须明确标注对比的两个具体日期；
- 严禁将周末或节假日误判为数据日期，若表格所示为周末，则应明确识别其实际所代表的前一个工作日数据，所有日期引用须精准对应真实交易或发布日；
- 若表中日期为周末或节假日，则仅用于展示目的，不得视作有效数据点，必须回溯至上一个真实交易日数据并据此撰写分析，所有引用与对比日期均须为有效数据时间点，不能伪造非交易日数据。"""}
    ],
    "temperature": 0.3
}

response = requests.post(API_URL, headers=headers, data=json.dumps(payload))
result = response.json()

#print(result["choices"][0]["message"]["content"])
markdown_text = result["choices"][0]["message"]["content"]
blocks = markdown_to_notion_blocks(markdown_text)

NOTION_TOKEN = "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"
DATABASE_ID = "2ca74daad637803fa61cf006e907b5eb"

url = "https://api.notion.com/v1/pages"

headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

payload = {
    "parent": {
        "database_id": DATABASE_ID
    },
    "properties": {
        # Title 属性（必须）
        "Name": {
            "title": [
                {
                    "text": {
                        "content": f"Liquidity (Day) - {datetime.now().strftime('%Y-%m-%d')}"
                    }
                }
            ]
        },

        # Multi-select / Tag
        "Tags": {
            "multi_select": [
                {"name": "AI报告"},
                {"name": "流动性报告"}
            ]
        },

        # Date
        "Report Date": {
            "date": {
                "start": datetime.now().strftime("%Y-%m-%d")
            }
        }
    },
    "children": blocks
            
}

response = requests.post(url, headers=headers, data=json.dumps(payload))

#print(response.status_code)
#print(response.json())