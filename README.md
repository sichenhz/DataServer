### 如何使用-SCSideBarController

![icon](http://img01.taobaocdn.com/imgextra/i1/135480037/TB2dK5KcXXXXXXTXpXXXXXXXXXX_!!135480037.gif)

####创建新类, 导入主头文件并继承

    #import "SCSideBarController.h"
    @interface ViewController : SCSideBarController
    @end

1.实例化一些控制器, 并添加为当前类的子控制器

    UIViewController *vc = [[UIViewController alloc] init];
    [self addChildViewController:vc];
