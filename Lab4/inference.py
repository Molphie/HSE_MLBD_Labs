import cv2
import pandas as pd
from ultralytics import YOLO

MODEL_PATH   = 'runs/train/plate_detector1/weights/best.pt'
VIDEO_PATH   = 'SHOT0005.MP4'
OUTPUT_CSV   = 'results.csv'
INTERVAL_SEC = 0.5
CONF_THRESH  = 0.25

def run_inference(model_path, video_path, output_csv, interval_sec=0.5, conf_thresh=0.25):

    model = YOLO(model_path)

    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    interval_frames = int(round(interval_sec * fps))

    records = []
    frame_idx = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break

        if frame_idx % interval_frames == 0:
            timestamp = frame_idx / fps
            res = model.predict(source=frame, conf=conf_thresh, verbose=False)[0]

            if len(res.boxes) > 0:
                for box in res.boxes:
                    cls_id = int(box.cls[0])
                    label  = model.names[cls_id]
                    x1, y1, x2, y2 = box.xyxy[0].tolist()
                    state = 'broken' if 'broken' in label else 'entire'
                    records.append({
                        'video_filename': video_path,
                        'target_type': label,
                        'bbox_x1': x1,
                        'bbox_y1': y1,
                        'bbox_x2': x2,
                        'bbox_y2': y2,
                        'state': state,
                        'timestamp': timestamp
                    })
            else:
                records.append({
                    'video_filename': video_path,
                    'target_type': None,
                    'bbox_x1': None,
                    'bbox_y1': None,
                    'bbox_x2': None,
                    'bbox_y2': None,
                    'state': 'none',
                    'timestamp': timestamp
                })

        frame_idx += 1

    cap.release()

    df = pd.DataFrame(records)
    df.to_csv(output_csv, index=False)
    print(f'Сохранено {len(records)} записей в {output_csv}')

if __name__ == '__main__':
    run_inference(MODEL_PATH, VIDEO_PATH, OUTPUT_CSV, INTERVAL_SEC, CONF_THRESH)
