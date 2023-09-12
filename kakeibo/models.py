from django.db import models


class PaymentCategory(models.Model):
    """支出カテゴリ"""
    name = models.CharField('カテゴリ名', max_length=32)

    def __str__(self):
        return self.name

class PaymentType(models.Model):
    """支払い方法"""
    name = models.CharField('カテゴリ名', max_length=32)

    def __str__(self):
        return self.name

class KouzaType(models.Model):
    """口座種別"""
    name = models.CharField('カテゴリ名', max_length=32)

    def __str__(self):
        return self.name


class Payment(models.Model):
    """支出"""
    date = models.DateField('日付')
    price = models.IntegerField('金額')
    category = models.ForeignKey(PaymentCategory, on_delete=models.PROTECT, verbose_name='カテゴリ')
    paycategory = models.ForeignKey(PaymentType, on_delete=models.PROTECT, verbose_name='支払い方法')
    kouza = models.ForeignKey(KouzaType, on_delete=models.PROTECT, verbose_name='口座種別')
    description = models.TextField('摘要', null=True, blank=True)


class IncomeCategory(models.Model):
    """収入カテゴリ"""
    name = models.CharField('カテゴリ名', max_length=32)

    def __str__(self):
        return self.name


class Income(models.Model):
    """収入"""
    date = models.DateField('日付')
    price = models.IntegerField('金額')
    category = models.ForeignKey(IncomeCategory, on_delete=models.PROTECT, verbose_name='カテゴリ')
    kouza = models.ForeignKey(KouzaType, on_delete=models.PROTECT, verbose_name='口座種別')
    description = models.TextField('摘要', null=True, blank=True)
