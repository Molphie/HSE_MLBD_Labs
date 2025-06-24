from ultralytics import YOLO

BASE_CKPT = 'runs/train/plate_detector/weights/best.pt'

model = YOLO(BASE_CKPT)

model.train(
    data='data.yaml',
    epochs=50,
    imgsz=640,
    batch=16,
    #classes=[0, 2],               # только dark_broken и red_broken
    project='runs/train', 
    name='plate_detector1',
    exist_ok=True,
    #freeze=10
)
