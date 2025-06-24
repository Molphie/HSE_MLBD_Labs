import os
import cv2
import pandas as pd
from ultralytics import YOLO

#MODEL_PATH   = 'runs/train/plate_detector_newimgsize/weights/best.pt'
MODEL_PATH   = 'runs/train/plate_detector1/weights/best.pt'
VIDEO_ROOT   = 'unmatched_data'
OUTPUT_CSV   = 'results_all.csv'
INTERVAL_SEC = 0.5
CONF_THRESH  = 0.25
VIDEO_EXTS   = ('.mp4', )

def find_videos(root_dir):
    video_paths = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fn in filenames:
            if fn.lower().endswith(VIDEO_EXTS):
                video_paths.append(os.path.join(dirpath, fn))
    return sorted(video_paths)

def process_video(model, video_path, interval_sec, conf_thresh):
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    interval_frames = int(round(interval_sec * fps))
    frame_idx = 0
    records = []

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
                        'video_filename': os.path.relpath(video_path, VIDEO_ROOT),
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
                    'video_filename': os.path.relpath(video_path, VIDEO_ROOT),
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
    return records

def main():
    model = YOLO(MODEL_PATH)

    videos = find_videos(VIDEO_ROOT)
    if not videos:
        print(f"Не найдено видео в {VIDEO_ROOT}")
        return

    all_records = []
    for vp in videos:
        print(f"Processing {vp} ...")
        recs = process_video(model, vp, INTERVAL_SEC, CONF_THRESH)
        print(f"  записей: {len(recs)}")
        all_records.extend(recs)

    df = pd.DataFrame(all_records)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nГотово! Всего записей: {len(all_records)} → {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
