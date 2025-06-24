from ultralytics import YOLO

model = YOLO('yolov8s.pt')

model.train(
    data='data.yaml',
    epochs=50,                 
    imgsz=640,
    #imgsz=1600,
    batch=16,                   
    workers=4,                 
    project='runs/train',       
    name='plate_detector',      
    exist_ok=True
)