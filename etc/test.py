import cv2

# 빈 창 생성
cv2.namedWindow("Test Window", cv2.WINDOW_NORMAL)
cv2.resizeWindow("Test Window", 300, 300)

while True:
    key = cv2.waitKey(0)  # 키 입력 대기
    if key == ord('q'):   # 'q'로 종료
        break
    print(f"Pressed key code: {key}")

cv2.destroyAllWindows()