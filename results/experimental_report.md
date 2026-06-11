# Báo cáo Đánh giá Thực nghiệm

Báo cáo này trình bày kết quả đo lường hiệu năng thực tế của Bộ lập lịch tác vụ phân tán (Distributed Task Scheduler).

## Thí nghiệm 1: Phân tích Khả năng mở rộng (Scalability)
**Thiết lập**: Chạy 100 tác vụ hỗn hợp. Các Worker được cấu hình chạy đơn luồng (1 CPU core).

| Số lượng Worker | Thời gian Hoàn thành (giây) | Thông lượng (Tác vụ/giây) | Tốc độ tăng tốc (Speedup) |
|:---:|:---:|:---:|:---:|
| 1 Worker | 0.41s | 246.28/s | 1.00x |
| 2 Workers | 0.20s | 489.82/s | 1.99x |
| 4 Workers | 0.10s | 974.94/s | 3.96x |
| 8 Workers | 0.10s | 967.86/s | 3.93x |

### Phát hiện chính
- **Biểu đồ trực quan**: [Biểu đồ SVG Khả năng mở rộng](file:///home/leminhtri-20224170/ex10/results/scalability_chart.svg)
- Khi tăng số lượng worker từ 1 lên 4, hệ thống ghi nhận thời gian hoàn thành giảm gần 4 lần và thông lượng tăng tuyến tính.
- Khi tăng lên 8 worker, hiệu năng bắt đầu bão hòa do kích thước tác vụ thử nghiệm tương đối nhỏ, chi phí thiết lập kết nối socket TCP và chuyển ngữ cảnh luồng trên cùng một máy chủ (localhost) chiếm tỷ trọng lớn hơn thời gian tính toán thực tế.

---

## Thí nghiệm 2: So sánh Thuật toán Lập lịch
**Thiết lập**: Chạy 25 tác vụ (5 tác vụ nặng, 20 tác vụ nhẹ) trên cụm 3 worker (mỗi worker cấu hình tối đa 2 lõi CPU song song).

| Thuật toán Lập lịch | Thời gian Phản hồi Trung bình (giây) |
|:---|:---:|
| FIFO | 0.007s |
| ROUND_ROBIN | 0.006s |
| LEAST_LOADED | 0.007s |

### Phát hiện chính
- **Biểu đồ trực quan**: [Biểu đồ SVG So sánh Thuật toán](file:///home/leminhtri-20224170/ex10/results/policy_chart.svg)
- **Least Loaded** (Tải thấp nhất) tự động phân bổ tác vụ dựa trên số lượng công việc đang xử lý thực tế của từng worker, tránh dồn ứ hàng đợi.
- **Round Robin** giao tác vụ tuần hoàn, đôi khi có thể phân bổ nhiều tác vụ nặng vào cùng một worker trong khi các worker khác rảnh rỗi.
- **FIFO** xử lý lần lượt theo thứ tự gửi đến, phân phối dựa trên thứ tự đăng ký của worker nên có độ lệch tải nhất định.

---

## Thí nghiệm 3: Khôi phục Lỗi (Failure Recovery)
**Thiết lập**: Chạy cụm 3 worker. Gửi 30 tác vụ tính toán. Khi đang xử lý, tiến trình Worker 2 bị tắt cưỡng bức (`kill -9`).

- **Result**: **THÀNH CÔNG** - Đã hoàn thành toàn bộ 30/30 tác vụ, không có tác vụ nào bị mất.
- **Quan sát**: Ngay sau khi phát hiện mất kết nối socket từ Worker 2, Master đã hủy trạng thái hoạt động của worker này, mở khóa tài nguyên, đưa 2 tác vụ đang chạy trên Worker 2 về trạng thái `READY` và lập lịch lại sang Worker 1 và Worker 3 để xử lý tiếp.

---
Báo cáo được lập tự động vào lúc: 2026-06-11 14:01:52
