# app_gradio.py
import gradio as gr
from pathlib import Path
from scripts.rag_multi_turn import init_agent  
# Khởi tạo agent 1 lần duy nhất khi app start
agent = init_agent()

# Đường dẫn file Excel để cho người dùng tải xuống
excel_path = Path("data/dữ liệu bán hàng.xlsx")

def chat_fn(message, history):
    default_prompt = 'Trả lời ngắn gọn, súc tích, dễ hiểu, không lặp lại câu hỏi bằng tiếng Việt. Nếu không biết thì nói "Tôi không biết", tuy nhiên cần xác minh lại một lần cuối trước khi đưa ra câu hỏi cuối cùng. Không nên để markdown khi không cần thiết.'
    try:
        answer = agent.run(message + "\n" + default_prompt)
    except Exception:
        answer = "Tôi không biết."
    return answer

with gr.Blocks() as demo:
    gr.Markdown("## CHATBOT HỖ TRỢ TRUY VẤN DỮ LIỆU BÁN HÀNG")

    with gr.Row():
        chatbot = gr.ChatInterface(fn=chat_fn)
    
    with gr.Row():
        if excel_path.exists():
            gr.File(
                value=str(excel_path),
                label="📂 Tải dữ liệu nguồn (RAG)",
                type="filepath",
                interactive=False
            )
        else:
            gr.Markdown("⚠️ Không tìm thấy file dữ liệu.")

demo.launch()
