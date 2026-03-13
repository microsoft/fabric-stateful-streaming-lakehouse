_Context: I have to deliver a 90 minute workshop on "Building Production-Ready Stateful Pipelines" in Microsoft Fabric Spark. I also plan to ship this content (minus some of the PowerPoint slides) as a "Jumpstart" in Fabric Jumpstart (see repo C:\Users\milescole\source\fabric-jumpstart\.github\copilot-instructions.md ) so that anyone can deploy this lab to test. I am using ArcFlow ( C:\Users\milescole\source\ArcFlow\.github\copilot-instructions.md ) as the example production ready ELT framework._

1. update explore_streaming.ipynb per the vision in vision.md Lab 2 Part 1
1. update arcflow_streaming_framework.ipynb per the vision in vision.md Lab 2 Part 2

Core requirements:
- Build for attendees having a delightful experience.
- It is a L300 workshop lab but we may have people less comfortable with Spark. The expectation is that people will be challenged but always have any easy button (see Challenge and Answer examples in streaming_demo.md) so they aren't blocked.
- Dependencies between parts of the lab need to be minimized so that if someone gets stuck on one area they aren't "hosed".
- Markdown should be visually engaging by not excessively use emojis.
- While I will have supplimental PPT slides in some areas where noted. The core concepts and learnings need to be achieved via these Notebooks.
- Use streaming_demo.ipynb as an example of another lab that I did. This has about 50% overlap with the vision of explore_streaming.ipynb
