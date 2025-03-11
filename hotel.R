# Cài đặt và load các gói cần thiết (chỉ cần cài đặt một lần)
install.packages("sparklyr")
install.packages("dplyr")
install.packages("ggplot2")
install.packages("readr")

library(sparklyr)
library(dplyr)
library(ggplot2)
library(readr)

# Kết nối Spark (chạy trên máy cục bộ)
sc <- spark_connect(master = "local")

# Đọc dữ liệu từ file CSV vào Spark
hotel_data <- spark_read_csv(
  sc, 
  path = "hotel_booking_trends.csv", 
  infer_schema = TRUE, 
  header = TRUE
)

# Kiểm tra nhanh cấu trúc dữ liệu
hotel_data %>% glimpse()

# Xử lý dữ liệu: nhóm theo 'Year' và 'Category', tính trung bình của 'Percentage'
hotel_summary <- hotel_data %>%
  group_by(Year, Category) %>%
  summarise(Percentage = mean(Percentage, na.rm = TRUE)) %>%
  collect()  # Chuyển kết quả về R dưới dạng tibble

# Vẽ biểu đồ cột với ggplot2
ggplot(hotel_summary, aes(x = factor(Year), y = Percentage, fill = Category)) +
  geom_bar(stat = "identity", 
           position = position_dodge(width = 0.7), 
           width = 0.7, 
           color = "black") +
  scale_fill_brewer(palette = "Set2") +
  labs(title = "Xu hướng thuê đặt phòng khách sạn", 
       x = "Năm", 
       y = "Tỷ lệ (%)", 
       fill = "Loại đặt phòng") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Ngắt kết nối Spark khi hoàn tất
spark_disconnect(sc)
