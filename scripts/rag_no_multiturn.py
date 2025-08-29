from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.agents import initialize_agent, Tool, AgentType
from sqlalchemy import create_engine
from langchain_core.prompts import PromptTemplate

from typing import List, Optional

from langchain.chains.llm import LLMChain
from langchain_community.tools.sql_database.prompt import QUERY_CHECKER
from langchain_core.callbacks.manager import CallbackManagerForChainRun

INTERMEDIATE_STEPS_KEY = "intermediate_steps"
SQL_QUERY = "SQLQuery:"
SQL_RESULT = "SQLResult:"


# Kết nối MySQL
import yaml
with open("config/config.yaml") as f:
    cfg = yaml.safe_load(f)
conn_str = cfg["mysql_db"]["url"]
engine = create_engine(
    conn_str,
    connect_args={
        "ssl": {"ssl-mode": "REQUIRED"}
    }
)
db = SQLDatabase(engine)

# Tạo SQLDatabaseChain
llm = ChatGoogleGenerativeAI(
    google_api_key=cfg["gemini"]["api_key"],
    model=cfg["gemini"]["model"], 
    temperature=0)

### Sử dụng prompt cho SQLDatabaseChain để kiểm soát chặt chẽ hơn truy vấn SQL (Bản chất thì trong SQLDatabaseChain cũng chứa 1 LLM)
_SQL_PROMPT = """Bạn là một chuyên gia MySQL. Nhiệm vụ của bạn là tạo ra một truy vấn MySQL HỢP LỆ VÀ NGUYÊN BẢN (raw SQL) dựa trên câu hỏi của người dùng.

QUY TẮC CỰC KỲ QUAN TRỌNG:
1.  **KHÔNG BỌC TRUY VẤN SQL TRONG MARKDOWN.**
2.  **Sử dụng DẤU BACKTICK (`)** cho tất cả tên bảng/cột có ký tự đặc biệt.
3.  **Luôn sử dụng single quotes cho giá trị chuỗi.** Ví dụ: 'Hà Nội' — không dùng "Hà Nội".
4.  **Luôn giới hạn truy vấn tối đa 10 kết quả** trừ khi người dùng chỉ định khác.
5.  **Chỉ truy vấn các cột cần thiết**, không SELECT *.
6.  **Kiểm tra kỹ cú pháp MySQL** và đảm bảo các tên bảng/cột là chính xác theo lược đồ.
7.  **Vấn đề ngày tháng cần đúng định dạng. Ví dụ: nếu nói tháng 8-2023 thì phải chuyển về lấy từ '2023-08-01' đến '2023-08-31' cho DATE, không dùng '2023-08' hay '2023'...**

Dựa trên các thông tin bảng sau:
{table_info}
Câu hỏi của người dùng: {input}
Câu trả lời:"""


SQL_PROMPT = PromptTemplate(
    input_variables=["input", "table_info"], # Lưu ý tên input_variables mới
    template=_SQL_PROMPT,
)

class CustomSQLDatabaseChain(SQLDatabaseChain):
    def _call(
        self,
        inputs: dict,
        run_manager: Optional[CallbackManagerForChainRun] = None,
    ) -> dict:
        _run_manager = run_manager or CallbackManagerForChainRun.get_noop_manager()

        # Lấy input
        input_text = f"{inputs[self.input_key]}\n{SQL_QUERY}"
        _run_manager.on_text(input_text, verbose=self.verbose)

        # Thông tin bảng
        table_names_to_use = inputs.get("table_names_to_use")
        table_info = self.database.get_table_info(table_names=table_names_to_use)

        llm_inputs = {
            "input": input_text,
            "top_k": str(self.top_k),
            "dialect": self.database.dialect,
            "table_info": table_info,
            "stop": ["\nSQLResult:"],
        }

        if self.memory is not None:
            for k in self.memory.memory_variables:
                llm_inputs[k] = inputs[k]

        intermediate_steps: List = []

        try:
            intermediate_steps.append(llm_inputs.copy())

            ############## Lần gọi đầu tiên
            sql_cmd = self.llm_chain.predict(
                callbacks=_run_manager.get_child(),
                **llm_inputs,
            ).strip()

            

            if self.return_sql:
                return {self.output_key: sql_cmd}

            # Phần còn lại giống gốc
            if not self.use_query_checker:
                _run_manager.on_text(sql_cmd, color="green", verbose=self.verbose)
                intermediate_steps.append(sql_cmd)
                intermediate_steps.append({"sql_cmd": sql_cmd})

                if SQL_QUERY in sql_cmd:
                    sql_cmd = sql_cmd.split(SQL_QUERY)[1].strip()
                if SQL_RESULT in sql_cmd:
                    sql_cmd = sql_cmd.split(SQL_RESULT)[0].strip()

                result = self.database.run(sql_cmd)
                intermediate_steps.append(str(result))

            else:
                # logic query_checker giữ nguyên
                query_checker_prompt = self.query_checker_prompt or PromptTemplate(
                    template=QUERY_CHECKER, input_variables=["query", "dialect"]
                )
                query_checker_chain = LLMChain(
                    llm=self.llm_chain.llm, prompt=query_checker_prompt
                )
                query_checker_inputs = {"query": sql_cmd, "dialect": self.database.dialect}
                checked_sql_command: str = query_checker_chain.predict(
                    callbacks=_run_manager.get_child(), **query_checker_inputs
                ).strip()
                
                # Clean markdown nếu cần
                if "```sql" in checked_sql_command:
                    checked_sql_command = checked_sql_command.replace("```sql", "").replace("```", "").strip()
                    print("💬 Cleaned SQL (checker):\n", checked_sql_command)
                
                intermediate_steps.append(checked_sql_command)
                _run_manager.on_text(checked_sql_command, color="green", verbose=self.verbose)
                intermediate_steps.append({"sql_cmd": checked_sql_command})
                result = self.database.run(checked_sql_command)
                intermediate_steps.append(str(result))
                sql_cmd = checked_sql_command

            _run_manager.on_text("\nSQLResult: ", verbose=self.verbose)
            _run_manager.on_text(str(result), color="yellow", verbose=self.verbose)

            # Return final result
            if self.return_direct:
                final_result = result
            else:
                _run_manager.on_text("\nAnswer:", verbose=self.verbose)
                input_text += f"{sql_cmd}\nSQLResult: {result}\nAnswer:"
                llm_inputs["input"] = input_text
                intermediate_steps.append(llm_inputs.copy())
                final_result = self.llm_chain.predict(
                    callbacks=_run_manager.get_child(),
                    **llm_inputs,
                ).strip()
                intermediate_steps.append(final_result)
                _run_manager.on_text(final_result, color="green", verbose=self.verbose)

            chain_result: dict = {self.output_key: final_result}
            if self.return_intermediate_steps:
                chain_result[INTERMEDIATE_STEPS_KEY] = intermediate_steps
            return chain_result

        except Exception as exc:
            exc.intermediate_steps = intermediate_steps
            raise exc


# Thay thế SQLDatabaseChain cũ bằng custom chain
sql_chain = CustomSQLDatabaseChain.from_llm(
    llm,
    db,
    prompt=SQL_PROMPT,
    verbose=True,
    use_query_checker=True,
    return_direct=True,  # Trả về kết quả trực tiếp không qua format
    #return_sql=True
)



### Agent gồm LLM và tool. Tool ở đây là sql_chain (tức là có 2 con LLMs)
# Tool cho Agent
tools = [
    Tool(
        name="SQL Database",
        func=sql_chain.run,
        description=(
            "Dùng để trả lời câu hỏi liên quan dữ liệu bán hàng, khách hàng, sản phẩm, "
            "nhân viên, chi nhánh và KPI. LLM cần tự xác định bảng nào liên quan, join như thế nào "
            "và tính toán aggregate khi cần."
        )
    )
]

# Khởi tạo Agent
agent = initialize_agent(
    tools,
    llm,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True,
    handle_parsing_errors=True,
    max_iterations=5,  # Giới hạn số vòng lặp để tránh quá dài
)

# Query thử
if __name__ == "__main__":
    while True:
        question = input("Nhập câu hỏi (hoặc 'exit' để thoát): ")
        if question.lower() in ["exit", "quit"]:
            break
        default_ = 'Trả lời ngắn gọn, súc tích, dễ hiểu, không lặp lại câu hỏi bằng tiếng Việt. Nếu không biết thì nói "Tôi không biết".'
        answer = agent.run(question + "\n" + default_)
        print("💬 Trả lời:", answer)
